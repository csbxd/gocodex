#ifndef GOCODEX_BRIDGE_H
#define GOCODEX_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Borrowed, NUL-terminated string with static lifetime. Do not free it. */
const char *gocodex_bridge_version(void);

/* Rust-owned allocation. Pass exactly once to gocodex_buffer_free. */
typedef struct {
    uint8_t *data;
    size_t len;
} gocodex_buffer;

typedef struct {
    uint64_t handle;
    int32_t code; /* 0: success, 1: error JSON in buffer, 2: EOF. */
    gocodex_buffer buffer;
} gocodex_result;

void gocodex_buffer_free(gocodex_buffer buffer);

/* Inputs are borrowed only during the call. NULL requires length 0.
 * Handles are never pointers. Close is idempotent and safe during send/read.
 * Every result.buffer, including errors, must be freed by the caller. */
gocodex_result gocodex_http_client_new(const uint8_t *config, size_t len);
gocodex_result gocodex_http_client_close(uint64_t client);
gocodex_result gocodex_http_request_new(uint64_t client,
    const uint8_t *metadata, size_t metadata_len,
    const uint8_t *body, size_t body_len);
gocodex_result gocodex_http_request_send(uint64_t request);
gocodex_result gocodex_http_response_read(uint64_t request, size_t max_len);
gocodex_result gocodex_http_request_close(uint64_t request);

gocodex_result gocodex_ws_client_new(const uint8_t *config, size_t len);
gocodex_result gocodex_ws_client_close(uint64_t client);
gocodex_result gocodex_ws_connection_new(uint64_t client, const uint8_t *metadata, size_t len);
gocodex_result gocodex_ws_connect(uint64_t connection);
gocodex_result gocodex_ws_read(uint64_t connection);
gocodex_result gocodex_ws_write(uint64_t connection, uint8_t kind, const uint8_t *data, size_t len);
gocodex_result gocodex_ws_connection_close(uint64_t connection);

#ifdef __cplusplus
}
#endif

#endif
