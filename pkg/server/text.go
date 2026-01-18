package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mevdschee/tqcache/pkg/tqcache"
)

const (
	maxKeyLength  = 250         // Memcached max key size
	maxLineLength = 2 * 1024    // Max command line length before closing connection
	maxValueSize  = 1024 * 1024 // Memcached default max item size (1MB)
)

func (s *Server) handleText(reader *bufio.Reader, writer *bufio.Writer) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			s.handleTextStorage(reader, writer, parts, "SET")
		case "ADD":
			s.handleTextStorage(reader, writer, parts, "ADD")
		case "REPLACE":
			s.handleTextStorage(reader, writer, parts, "REPLACE")
		case "APPEND":
			s.handleTextAppendPrepend(reader, writer, parts, false)
		case "PREPEND":
			s.handleTextAppendPrepend(reader, writer, parts, true)
		case "CAS":
			s.handleTextCas(reader, writer, parts)
		case "GET":
			s.handleTextGet(writer, parts, false)
		case "GETS":
			s.handleTextGet(writer, parts, true)
		case "DELETE":
			s.handleTextDelete(writer, parts)
		case "INCR":
			s.handleTextIncrDecr(writer, parts, true)
		case "DECR":
			s.handleTextIncrDecr(writer, parts, false)
		case "TOUCH":
			s.handleTextTouch(writer, parts)
		case "GAT":
			s.handleTextGat(writer, parts, false)
		case "GATS":
			s.handleTextGat(writer, parts, true)
		case "FLUSH_ALL":
			s.handleTextFlushAll(writer, parts)
		case "VERBOSITY":
			// Silently accept verbosity command (noreply handled implicitly)
		case "QUIT":
			return
		case "VERSION":
			writer.WriteString("VERSION 1.0.0\r\n")
		case "STATS":
			s.handleTextStats(writer)
		default:
			writer.WriteString("ERROR\r\n")
		}

		// Flush once per command (batched writes)
		if reader.Buffered() == 0 {
			writer.Flush()
		}
	}
}

func (s *Server) handleTextStorage(reader *bufio.Reader, writer *bufio.Writer, parts []string, op string) {
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	key := parts[1]
	// Validate flags (must be numeric)
	_, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	// Validate exptime (must be numeric)
	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	// Validate bytes (must be numeric)
	bytes, err := strconv.Atoi(parts[4])
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	// Check value size limit (Memcached default is 1MB)
	if bytes > maxValueSize {
		// Still need to read and discard the data
		discard := make([]byte, bytes)
		io.ReadFull(reader, discard)
		reader.ReadByte() // \r
		reader.ReadByte() // \n
		writer.WriteString("SERVER_ERROR object too large for cache\r\n")
		return
	}
	noreply := len(parts) > 5 && parts[5] == "noreply"

	// Read value
	value := make([]byte, bytes)
	if _, err := io.ReadFull(reader, value); err != nil {
		writer.WriteString("SERVER_ERROR read error\r\n")
		return
	}

	// Read \r\n
	c, _ := reader.ReadByte()
	if c == '\r' {
		reader.ReadByte()
	}

	// Calculate TTL
	var ttl time.Duration
	if exptime < 0 {
		// Negative exptime means already expired
		ttl = time.Nanosecond
	} else if exptime > 0 {
		if exptime > 2592000 {
			// Unix timestamp
			ttl = time.Until(time.Unix(exptime, 0))
			if ttl <= 0 {
				// Timestamp is in the past, already expired
				ttl = time.Nanosecond
			}
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	switch op {
	case "SET":
		_, err = s.cache.Set(key, value, ttl)
	case "ADD":
		_, err = s.cache.Add(key, value, ttl)
	case "REPLACE":
		_, err = s.cache.Replace(key, value, ttl)
	}

	if err != nil {
		if err == tqcache.ErrKeyExists || err == tqcache.ErrKeyNotFound {
			if !noreply {
				writer.WriteString("NOT_STORED\r\n")
			}
			return
		}
		writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
		return
	}

	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (s *Server) handleTextCas(reader *bufio.Reader, writer *bufio.Writer, parts []string) {
	// Need at least 5 parts to parse bytes (key, flags, exptime, bytes)
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	key := parts[1]
	// Validate flags (must be numeric)
	_, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	// Validate exptime (must be numeric)
	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	// Validate bytes (must be numeric)
	bytes, err := strconv.Atoi(parts[4])
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	// Read value (must always consume the data to stay in sync)
	value := make([]byte, bytes)
	if _, err2 := io.ReadFull(reader, value); err2 != nil {
		writer.WriteString("SERVER_ERROR read error\r\n")
		return
	}
	// Read \r\n
	c, _ := reader.ReadByte()
	if c == '\r' {
		reader.ReadByte()
	}

	// Now check if cas token is present and valid
	if len(parts) < 6 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	casToken, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	noreply := len(parts) > 6 && parts[6] == "noreply"

	// Calculate TTL
	var ttl time.Duration
	if exptime < 0 {
		// Negative exptime means already expired
		ttl = time.Nanosecond
	} else if exptime > 0 {
		if exptime > 2592000 {
			// Unix timestamp
			ttl = time.Until(time.Unix(exptime, 0))
			if ttl <= 0 {
				// Timestamp is in the past, already expired
				ttl = time.Nanosecond
			}
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	_, err = s.cache.Cas(key, value, ttl, casToken)
	if err != nil {
		if err == tqcache.ErrCasMismatch {
			if !noreply {
				writer.WriteString("EXISTS\r\n")
			}
			return
		}
		if err == tqcache.ErrKeyNotFound {
			if !noreply {
				writer.WriteString("NOT_FOUND\r\n")
			}
			return
		}
		writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
		return
	}

	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (s *Server) handleTextGet(writer *bufio.Writer, parts []string, withCas bool) {
	if len(parts) < 2 {
		writer.WriteString("ERROR\r\n")
		return
	}

	for _, key := range parts[1:] {
		value, cas, err := s.cache.Get(key)
		if err == nil {
			writer.WriteString("VALUE ")
			writer.WriteString(key)
			writer.WriteString(" 0 ")
			writer.WriteString(strconv.Itoa(len(value)))
			if withCas {
				writer.WriteString(" ")
				writer.WriteString(strconv.FormatUint(cas, 10))
			}
			writer.WriteString("\r\n")
			writer.Write(value)
			writer.WriteString("\r\n")
		}
	}
	writer.WriteString("END\r\n")
}

func (s *Server) handleTextDelete(writer *bufio.Writer, parts []string) {
	if len(parts) < 2 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	key := parts[1]
	noreply := len(parts) > 2 && parts[2] == "noreply"

	err := s.cache.Delete(key)
	if err == nil {
		if !noreply {
			writer.WriteString("DELETED\r\n")
		}
	} else {
		if !noreply {
			writer.WriteString("NOT_FOUND\r\n")
		}
	}
}

func (s *Server) handleTextIncrDecr(writer *bufio.Writer, parts []string, incr bool) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	key := parts[1]
	valStr := parts[2]
	delta, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR invalid numeric delta argument\r\n")
		return
	}
	noreply := len(parts) > 3 && parts[3] == "noreply"

	var newVal uint64
	if incr {
		newVal, _, err = s.cache.Increment(key, delta)
	} else {
		newVal, _, err = s.cache.Decrement(key, delta)
	}

	if err != nil {
		if err == tqcache.ErrKeyNotFound {
			if !noreply {
				writer.WriteString("NOT_FOUND\r\n")
			}
			return
		}
		writer.WriteString("CLIENT_ERROR " + err.Error() + "\r\n")
		return
	}

	if !noreply {
		writer.WriteString(strconv.FormatUint(newVal, 10) + "\r\n")
	}
}

func (s *Server) handleTextTouch(writer *bufio.Writer, parts []string) {
	if len(parts) < 3 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	key := parts[1]
	exptime, _ := strconv.ParseInt(parts[2], 10, 64)
	noreply := len(parts) > 3 && parts[3] == "noreply"

	var ttl time.Duration
	if exptime < 0 {
		// Negative exptime means already expired
		ttl = time.Nanosecond
	} else if exptime > 0 {
		if exptime > 2592000 {
			// Unix timestamp
			ttl = time.Until(time.Unix(exptime, 0))
			if ttl <= 0 {
				// Timestamp is in the past, already expired
				ttl = time.Nanosecond
			}
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	_, err := s.cache.Touch(key, ttl)
	if err != nil {
		if !noreply {
			if err == tqcache.ErrKeyNotFound {
				writer.WriteString("NOT_FOUND\r\n")
			} else {
				writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
			}
		}
		return
	}

	if !noreply {
		writer.WriteString("TOUCHED\r\n")
	}
}

// handleTextGat handles GAT (get and touch) and GATS commands
func (s *Server) handleTextGat(writer *bufio.Writer, parts []string, withCas bool) {
	if len(parts) < 3 {
		writer.WriteString("ERROR\r\n")
		return
	}

	exptime, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	// Calculate TTL
	var ttl time.Duration
	if exptime < 0 {
		ttl = time.Nanosecond
	} else if exptime > 0 {
		if exptime > 2592000 {
			ttl = time.Until(time.Unix(exptime, 0))
			if ttl <= 0 {
				ttl = time.Nanosecond
			}
		} else {
			ttl = time.Duration(exptime) * time.Second
		}
	}

	// Process each key
	for _, key := range parts[2:] {
		// Get the value first (before touching with potentially expired TTL)
		value, cas, err := s.cache.Get(key)
		if err != nil {
			continue // Key not found, skip
		}

		// Now touch with new expiry
		s.cache.Touch(key, ttl)

		// Output the value
		writer.WriteString("VALUE ")
		writer.WriteString(key)
		writer.WriteString(" 0 ")
		writer.WriteString(strconv.Itoa(len(value)))
		if withCas {
			writer.WriteString(" ")
			writer.WriteString(strconv.FormatUint(cas, 10))
		}
		writer.WriteString("\r\n")
		writer.Write(value)
		writer.WriteString("\r\n")
	}
	writer.WriteString("END\r\n")
}

func (s *Server) handleTextFlushAll(writer *bufio.Writer, parts []string) {
	noreply := false
	for _, p := range parts[1:] {
		if p == "noreply" {
			noreply = true
		}
	}

	s.cache.FlushAll()
	if !noreply {
		writer.WriteString("OK\r\n")
	}
}

func (s *Server) handleTextAppendPrepend(reader *bufio.Reader, writer *bufio.Writer, parts []string, prepend bool) {
	// append/prepend <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
	if len(parts) < 5 {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}

	key := parts[1]
	// Validate bytes (must be numeric)
	bytes, err := strconv.Atoi(parts[4])
	if err != nil {
		writer.WriteString("CLIENT_ERROR bad command line format\r\n")
		return
	}
	noreply := len(parts) > 5 && parts[5] == "noreply"

	// Read value
	value := make([]byte, bytes)
	if _, err := io.ReadFull(reader, value); err != nil {
		writer.WriteString("SERVER_ERROR read error\r\n")
		return
	}

	// Read \r\n
	c, _ := reader.ReadByte()
	if c == '\r' {
		reader.ReadByte()
	}

	// Call cache append/prepend
	if prepend {
		_, err = s.cache.Prepend(key, value)
	} else {
		_, err = s.cache.Append(key, value)
	}

	if err != nil {
		if err == tqcache.ErrKeyNotFound {
			if !noreply {
				writer.WriteString("NOT_STORED\r\n")
			}
			return
		}
		writer.WriteString("SERVER_ERROR " + err.Error() + "\r\n")
		return
	}

	if !noreply {
		writer.WriteString("STORED\r\n")
	}
}

func (s *Server) handleTextStats(writer *bufio.Writer) {
	stats := s.cache.Stats()
	writer.WriteString(fmt.Sprintf("STAT pid %d\r\n", os.Getpid()))
	writer.WriteString(fmt.Sprintf("STAT uptime %d\r\n", int64(time.Since(s.cache.GetStartTime()).Seconds())))
	writer.WriteString(fmt.Sprintf("STAT time %d\r\n", time.Now().Unix()))
	writer.WriteString("STAT version 1.0.0\r\n")
	for k, v := range stats {
		writer.WriteString(fmt.Sprintf("STAT %s %s\r\n", k, v))
	}
	writer.WriteString("END\r\n")
}
