<?php

$requestId = $_SERVER['HTTP_X_RETRY_TEST_ID'] ?? '';
if ($requestId === '') {
    http_response_code(400);
    echo "missing X-Retry-Test-ID";
    return;
}

$countFile = sys_get_temp_dir() . '/retry-response-' . hash('sha256', $requestId) . '.txt';
$attempt = file_exists($countFile) ? ((int) file_get_contents($countFile)) + 1 : 1;
file_put_contents($countFile, (string) $attempt);

if (($_GET['mode'] ?? '') === 'upload') {
    if (!isset($_FILES['upload'])) {
        http_response_code(400);
        echo "missing upload";
        return;
    }

    $upload = $_FILES['upload'];
    $tmpName = $upload['tmp_name'] ?? '';
    $movedPath = sys_get_temp_dir() . '/retry-response-moved-' . hash('sha256', $requestId);
    $tmpExists = $tmpName !== '' && file_exists($tmpName);
    $hash = $tmpExists ? hash_file('sha256', $tmpName) : '';

    header('X-PHP-Attempt: ' . $attempt);
    header('X-Upload-Error: ' . ($upload['error'] ?? 'missing'));
    header('X-Upload-Size: ' . ($upload['size'] ?? 'missing'));
    header('X-Upload-SHA256: ' . $hash);
    header('X-Upload-Tmp-Exists: ' . ($tmpExists ? 'yes' : 'no'));
    header('X-Upload-Moved-Exists: ' . (file_exists($movedPath) ? 'yes' : 'no'));

    if ($attempt === 1) {
        if ($tmpExists && !move_uploaded_file($tmpName, $movedPath)) {
            http_response_code(500);
            echo "move_uploaded_file failed";
            return;
        }

        header('Set-Cookie: upload-leak=1');
        http_response_code(429);
        echo "discarded upload attempt";
        return;
    }

    http_response_code(200);
    echo "upload-attempt=" . $attempt . "\n";
    echo $upload['name'] . "\n";
    echo $hash;
    return;
}

$body = file_get_contents('php://input');

header('X-PHP-Attempt: ' . $attempt);
header('X-Body-SHA256: ' . hash('sha256', $body));
header('X-Request-Method: ' . $_SERVER['REQUEST_METHOD']);
header('X-Content-Length-Seen: ' . ($_SERVER['CONTENT_LENGTH'] ?? ''));

if ($attempt === 1) {
    header('Set-Cookie: leak=1');
    http_response_code(429);
    echo "discarded attempt";
    return;
}

http_response_code(200);
echo "attempt=" . $attempt . "\n";
echo $body;
