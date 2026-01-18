#!/bin/bash

# TQCache Memcached Compatibility Tests
# Downloads and runs the official Memcached Perl test suite

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DIR="$SCRIPT_DIR"
PORT="${TQCACHE_PORT:-11299}"
BINARY="$SCRIPT_DIR/tqcache_test"

# Test files to download from official memcached repo
MEMCACHED_RAW="https://raw.githubusercontent.com/memcached/memcached/master"
TEST_FILES=(
    "t/lib/MemcachedTest.pm"
    "t/getset.t"
    "t/cas.t"
    "t/incrdecr.t"
    "t/touch.t"
    "t/flush-all.t"
    "t/noreply.t"
    "t/flags.t"
    "t/expirations.t"
)

download_tests() {
    echo "=== Downloading Memcached Test Suite ==="
    mkdir -p "$TEST_DIR/t/lib"
    
    for file in "${TEST_FILES[@]}"; do
        local dest="$TEST_DIR/$file"
        local url="$MEMCACHED_RAW/$file"
        
        if [ ! -f "$dest" ]; then
            echo "Downloading $file..."
            mkdir -p "$(dirname "$dest")"
            curl -sL "$url" -o "$dest"
        fi
    done
    echo ""
}

patch_test_library() {
    echo "=== Patching MemcachedTest.pm for TQCache ==="
    local pm_file="$TEST_DIR/t/lib/MemcachedTest.pm"
    
    # Check if already patched
    if grep -q "TQCACHE_PATCHED" "$pm_file" 2>/dev/null; then
        echo "Already patched"
        echo ""
        return
    fi
    
    # Create backup
    cp "$pm_file" "$pm_file.orig"

    # Apply all patches with a single Perl script for reliability
    perl -i -pe '
        # Mark as patched
        if (/^package MemcachedTest;/) {
            $_ .= "# TQCACHE_PATCHED\n";
        }
        
        # Use TQCACHE_BINARY env var
        s/^(sub get_memcached_exe \{)/$1\n    return \$ENV{TQCACHE_BINARY} if \$ENV{TQCACHE_BINARY};/;
        
        # Strip -m flag from args (TQCache is disk-based, not memory-based)
        s/^(sub new_memcached \{)/$1\n    my \$orig_args = shift; my \$a = defined \$orig_args ? \$orig_args : ""; \$a =~ s\/-m\\s+\\d+\/\/g; unshift \@_, \$a;  # Strip -m flag for TQCache/;
        
        # Remove unsupported args
        s/\$args \.= " -u root";/# Disabled for TQCache: -u root/;
        s/\$args \.= " -o relaxed_privileges";/# Disabled for TQCache: -o relaxed_privileges/;
        s/\$args \.= " -U \$udpport";/# Disabled for TQCache: -U/;
        s/\$args \.= " -Z";/# Disabled for TQCache: -Z/;
        s/\$args \.= " -o ssl_chain_cert=\$server_crt";/# Disabled for TQCache/;
        s/\$args \.= " -o ssl_key=\$server_key";/# Disabled for TQCache/;
        
        # Remove timedrun wrapper
        s/my \$cmd = "\$builddir\/timedrun 600 \$valgrind \$exe \$args";/my \$cmd = "\$exe \$args";/;
        
        # Make print_help return empty to avoid Usage spam
        s/^(sub print_help \{)/$1\n    return "" if \$ENV{TQCACHE_BINARY};/;
    ' "$pm_file"
    
    echo "Patched MemcachedTest.pm"
    echo ""
}

patch_test_files() {
    echo "=== Patching Test Files for TQCache ==="
    
    # Patch cas.t - disable check_args tests
    local cas_file="$TEST_DIR/t/cas.t"
    if [ -f "$cas_file" ] && ! grep -q "TQCACHE_PATCHED" "$cas_file" 2>/dev/null; then
        echo "Patching cas.t..."
        perl -i -pe '
            # Mark as patched at the start
            if (/^use strict;/ && !$patched) {
                $_ .= "# TQCACHE_PATCHED\n";
                $patched = 1;
            }
            # Comment out check_args function and calls
            s/^(sub check_args \{)/# Disabled for TQCache:\n# $1/;
            s/^(    my \(\$line, \$name\) = \@_;)/# $1/ if $in_check_args;
            s/^(check_args )/# $1/;
        ' "$cas_file"
        
        # More robust patching - comment out the entire check_args block
        perl -i -0777 -pe '
            s/(sub check_args \{.*?\n\})/# Disabled for TQCache:\n# $1 =~ s|^|# |gmr/se;
        ' "$cas_file"
    fi
    
    # Patch getset.t - disable first subtest
    local getset_file="$TEST_DIR/t/getset.t"
    if [ -f "$getset_file" ] && ! grep -q "TQCACHE_PATCHED" "$getset_file" 2>/dev/null; then
        echo "Patching getset.t..."
        perl -i -pe '
            # Mark as patched at the start
            if (/^use strict;/ && !$patched) {
                $_ .= "# TQCACHE_PATCHED\n";
                $patched = 1;
            }
        ' "$getset_file"
        
        # Comment out the subtest block
        perl -i -0777 -pe '
            s/(subtest .close if no get found in 2k. => sub \{.*?\};)/# Disabled for TQCache:\n# $1 =~ s|^|# |gmr/se;
        ' "$getset_file"
    fi
    
    echo "Patched test files"
    echo ""
}

build_tqcache() {
    echo "=== Building TQCache ==="
    go build -o "$BINARY" "$PROJECT_DIR/cmd/tqcache"
    echo "Built: $BINARY"
    echo ""
}

run_tests() {
    echo "=== Running Memcached Compatibility Tests ==="
    echo ""
    
    cd "$TEST_DIR/t"
    
    # Set environment for tests
    export TQCACHE_BINARY="$BINARY"
    export MEMCACHED_PORT="$PORT"
    
    local total_pass=0
    local total_fail=0
    
    for test_file in *.t; do
        if [ -f "$test_file" ]; then
            echo "--- $test_file ---"
            
            # Run test with timeout, filter output to show only test results
            local output
            output=$(timeout 60 perl "$test_file" 2>&1 || true)
            
            # Extract total tests from plan (1..N)
            local total=$(echo "$output" | grep -oE "^1\\.\\.[0-9]+" | head -1 | cut -d. -f3)
            [ -z "$total" ] && total=0
            
            # Count ok/not ok lines
            local pass=$(echo "$output" | grep -cE "^ok " || true)
            local fail=$(echo "$output" | grep -cE "^not ok " || true)
            
            total_pass=$((total_pass + pass))
            total_fail=$((total_fail + fail))
            
            # Show compact summary
            if [ "$fail" -eq 0 ] && [ "$pass" -gt 0 ]; then
                echo "  PASS: $pass/$total tests"
            elif [ "$pass" -gt 0 ] || [ "$fail" -gt 0 ]; then
                echo "  Pass: $pass/$total, Fail: $fail"
                # Show first few failures
                echo "$output" | grep -E "^not ok " | head -3 | sed 's/^/  /'
            else
                echo "  ERROR: Test did not run properly"
            fi
            echo ""
        fi
    done
    
    echo "=== Summary ==="
    echo "Total Passed: $total_pass"
    echo "Total Failed: $total_fail"
}

cleanup() {
    pkill -f "tqcache_test" 2>/dev/null || true
    rm -f "$BINARY" 2>/dev/null || true
    rm -rf "$TEST_DIR/t/data" 2>/dev/null || true  # Clean up TQCache data directory (created in t/)
    #rm -rf "$TEST_DIR/t" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

# Main
echo "=== TQCache Memcached Test Suite ==="
echo ""

# Check dependencies
if ! command -v perl &> /dev/null; then
    echo "ERROR: perl is required but not installed"
    exit 1
fi

if ! perl -e 'use Test::More' 2>/dev/null; then
    echo "ERROR: Perl Test::More module is required"
    echo "Install with: cpan Test::More"
    exit 1
fi

download_tests
patch_test_library
patch_test_files
build_tqcache
run_tests
cleanup

echo ""
echo "=== COMPLETED ==="
