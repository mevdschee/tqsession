<?php

$port = getenv('TQCACHE_PORT') ?: 11212;
$host = '127.0.0.1';

echo "Connecting to TQCache at $host:$port...\n";

$m = new Memcached();
$m->setOption(Memcached::OPT_CONNECT_TIMEOUT, 1000); // 1s
// $m->setOption(Memcached::OPT_BINARY_PROTOCOL, true); // Try Binary? 
// Retrying text first with more debug.
$m->addServer($host, $port);

$statuses = $m->getStats();
echo "Stats: " . print_r($statuses, true) . "\n";

$version = $m->getVersion();
if (empty($version)) {
    echo "FAILED: Could not connect to server.\n";
    echo "Result Code: " . $m->getResultCode() . "\n";
    echo "Result Msg: " . $m->getResultMessage() . "\n";
    exit(1);
}
echo "Connected. Server Version: " . print_r($version, true) . "\n";

// 1. SET / GET
echo "Test 1: SET/GET... ";
if (!$m->set('php_key', 'php_value', 10)) {
    echo "FAILED (Set)\n";
    exit(1);
}
$val = $m->get('php_key');
if ($val !== 'php_value') {
    echo "FAILED (Get mismatch: expected 'php_value', got '$val')\n";
    exit(1);
}
echo "PASSED\n";

// 2. ADD (Existing)
echo "Test 2: ADD (Existing)... ";
if ($m->add('php_key', 'new_value')) {
    echo "FAILED (Add succeeded on existing key)\n";
    exit(1);
}
echo "PASSED\n";

// 3. REPLACE
echo "Test 3: REPLACE... ";
if (!$m->replace('php_key', 'replaced_value')) {
    echo "FAILED (Replace returned false)\n";
    exit(1);
}
$val = $m->get('php_key');
if ($val !== 'replaced_value') {
    echo "FAILED (Replace mismatch)\n";
    exit(1);
}
echo "PASSED\n";

// 4. DELETE
echo "Test 4: DELETE... ";
if (!$m->delete('php_key')) {
    echo "FAILED (Delete returned false)\n";
    exit(1);
}
if ($m->get('php_key') !== false) {
    echo "FAILED (Key still exists after delete)\n";
    exit(1);
}
if ($m->getResultCode() !== Memcached::RES_NOTFOUND) {
    echo "FAILED (Expected RES_NOTFOUND)\n";
    exit(1);
}
echo "PASSED\n";

// 5. INCR / DECR
echo "Test 5: INCR/DECR... ";
$m->set('counter', 10);
$newVal = $m->increment('counter', 5);
if ($newVal !== 15) {
    echo "FAILED (Incr expected 15, got $newVal)\n";
    exit(1);
}
$newVal = $m->decrement('counter', 2);
if ($newVal !== 13) {
    echo "FAILED (Decr expected 13, got $newVal)\n";
    exit(1);
}
echo "PASSED\n";

// 6. CAS
echo "Test 6: CAS... ";
$m->set('cas_key', 'value1');
$flags = Memcached::GET_EXTENDED;
$result = $m->get('cas_key', null, $flags);
if (!isset($result['cas']) || empty($result['cas'])) {
    echo "FAILED (No CAS token in extended get)\n";
    print_r($result);
    exit(1);
}
$cas = $result['cas'];
// Try update with CAS
if (!$m->cas($cas, 'cas_key', 'value2')) {
    echo "FAILED (CAS update failed)\n";
    exit(1);
}
// Verify update
if ($m->get('cas_key') !== 'value2') {
    echo "FAILED (CAS update not applied)\n";
    exit(1);
}
// Try bad CAS
if ($m->cas($cas, 'cas_key', 'value3')) { // Old CAS
    echo "FAILED (CAS with old token succeeded)\n";
    exit(1);
}
if ($m->getResultCode() !== Memcached::RES_DATA_EXISTS) {
    echo "FAILED (Expected RES_DATA_EXISTS for bad CAS)\n";
    exit(1);
}
echo "PASSED\n";

// 7. TOUCH (Requires Memcached extension support, usually via touch())
echo "Test 7: TOUCH... ";
$m->set('touch_key', 'val', 2); // 2 seconds
$m->touch('touch_key', 100); // Extend to 100s
sleep(3);
if ($m->get('touch_key') !== 'val') {
    echo "FAILED (Item expired despite touch)\n";
    exit(1);
}
echo "PASSED\n";

// 8. Session Handler Test (Simulation)
// Since we are CLI, we can't fully check session handler integration hook, 
// but we can check if Binary protocol (session default) works for high churn.
echo "Test 8: Binary Protocol Heavy Load... ";
$mBin = new Memcached(); // New instance to ensure clean state buffer
$mBin->setOption(Memcached::OPT_BINARY_PROTOCOL, true);
$mBin->setOption(Memcached::OPT_CONNECT_TIMEOUT, 1000);
$mBin->addServer($host, $port);

for ($i = 0; $i < 100; $i++) {
    if (!$mBin->set("sess_$i", "data_$i", 3600)) {
        echo "FAILED (Binary load test SET at $i)\n";
        echo "Result Code: " . $mBin->getResultCode() . "\n";
        echo "Result Msg: " . $mBin->getResultMessage() . "\n";
        exit(1);
    }
}
for ($i = 0; $i < 100; $i++) {
    $got = $mBin->get("sess_$i");
    if ($got !== "data_$i") {
        echo "FAILED (Binary load test match at $i). Expected 'data_$i', got '" . var_export($got, true) . "'\n";
        echo "Result Code: " . $mBin->getResultCode() . "\n";
        echo "Result Msg: " . $mBin->getResultMessage() . "\n";
        exit(1);
    }
}
echo "PASSED\n";

echo "ALL TESTS PASSED\n";
exit(0);
