// ✓ c6 - 커스텀 에러 클래스 정의
'use strict';

class AppError extends Error {
  constructor(message, statusCode) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode || 500;
    Error.captureStackTrace(this, this.constructor);
  }
}

class NotFoundError extends AppError {
  constructor(message) {
    super(message || 'Resource not found', 404);
  }
}

class ValidationError extends AppError {
  constructor(message) {
    super(message || 'Validation failed', 400);
  }
}

class ExternalServiceError extends AppError {
  constructor(message) {
    super(message || 'External service error', 502);
  }
}

// ✓ c5 - DatabaseError는 statusCode 503을 사용한다
class DatabaseError extends AppError {
  constructor(message) {
    super(message || 'Database error', 503);
  }
}

module.exports = { AppError, NotFoundError, ValidationError, ExternalServiceError, DatabaseError };
