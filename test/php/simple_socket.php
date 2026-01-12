<?php
$port = 11212;
$host = '127.0.0.1';
echo "Connecting to $host:$port...\n";
$fp = fsockopen($host, $port, $errno, $errstr, 5);
if (!$fp) {
    echo "ERROR: $errstr ($errno)\n";
    exit(1);
}
echo "Connected. Sending 'version'...\n";
fwrite($fp, "version\r\n");
echo "Read response...\n";
$line = fgets($fp);
echo "Response: $line\n";
fclose($fp);
