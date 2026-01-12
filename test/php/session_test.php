<?php

$port = getenv('TQCACHE_PORT') ?: 11212;
$host = '127.0.0.1';
$savePath = "$host:$port";

// Configure Session
ini_set('session.save_handler', 'memcached');
ini_set('session.save_path', $savePath);
ini_set('session.gc_maxlifetime', 3600); // 1 hour
ini_set('session.use_cookies', 0);       // Disable cookie sending
ini_set('session.use_only_cookies', 0);
ini_set('session.cache_limiter', '');    // Disable cache headers

echo "Configuring session to use memcached at $savePath...\n";

// Start Session 1
echo "Test 1: Start Session & Write Data... ";
if (!session_start()) {
    echo "FAILED (session_start)\n";
    exit(1);
}
$sessionId = session_id();
$_SESSION['user_id'] = 12345;
$_SESSION['username'] = 'tqcache_user';
$_SESSION['flash_msg'] = 'Hello World';
session_write_close(); // Force write to cache
echo "PASSED (Session ID: $sessionId)\n";

// Start Session 2 (Simulate new request)
echo "Test 2: Re-open Session & Read Data... ";
session_abort(); // Ensure clean slate
session_id($sessionId);
session_start();

if (!isset($_SESSION['user_id']) || $_SESSION['user_id'] !== 12345) {
    echo "FAILED (user_id mismatch)\n";
    print_r($_SESSION);
    exit(1);
}
if (!isset($_SESSION['username']) || $_SESSION['username'] !== 'tqcache_user') {
    echo "FAILED (username mismatch)\n";
    exit(1);
}
echo "PASSED\n";

// Modify Data
echo "Test 3: Modify Data... ";
$_SESSION['username'] = 'updated_user';
unset($_SESSION['flash_msg']);
session_write_close();
echo "PASSED\n";

// Start Session 3 (Verify Modification)
echo "Test 4: Verify Modification... ";
session_abort();
session_id($sessionId);
session_start();

if ($_SESSION['username'] !== 'updated_user') {
    echo "FAILED (username update lost)\n";
    exit(1);
}
if (isset($_SESSION['flash_msg'])) {
    echo "FAILED (flash_msg should be deleted)\n";
    exit(1);
}
echo "PASSED\n";

// Destroy Session
echo "Test 5: Destroy Session... ";
session_destroy();
echo "PASSED\n";

// Verify Destruction
echo "Test 6: Verify Destruction... ";
session_abort();
session_id($sessionId);
session_start();
if (!empty($_SESSION)) {
    echo "FAILED (Session should be empty after destroy)\n";
    print_r($_SESSION);
    exit(1);
}
echo "PASSED\n";

echo "ALL SESSION TESTS PASSED\n";
exit(0);
