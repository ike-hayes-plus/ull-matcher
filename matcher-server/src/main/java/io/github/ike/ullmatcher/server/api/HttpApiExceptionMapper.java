package io.github.ike.ullmatcher.server.api;

final class HttpApiExceptionMapper {
    private HttpApiExceptionMapper() {}

    static ServerApiException map(String operation, RuntimeException error) {
        if (error instanceof ServerApiException apiError) {
            return apiError;
        }
        if (error instanceof IllegalArgumentException e) {
            return new BadRequestException(e.getMessage(), e);
        }
        if (error instanceof IllegalStateException e) {
            return new ConflictException(e.getMessage() == null || e.getMessage().isBlank()
                    ? operation + " is not allowed in current state"
                    : e.getMessage());
        }
        return new InternalServerException(operation + " failed", error);
    }
}
