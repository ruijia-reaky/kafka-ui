package io.kafbat.ui.datahub;

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import io.kafbat.ui.exception.NotFoundException;
import io.kafbat.ui.exception.TopicNotFoundException;
import io.kafbat.ui.exception.ValidationException;

public final class DataHubErrors {

  private DataHubErrors() {
  }

  /**
   * SDK messages contain request ids and resource paths, but never credentials.
   */
  public static String describe(Throwable e) {
    if (e instanceof DatahubClientException de) {
      return de.getErrorCode() == null
          ? "DataHub error: %s".formatted(de.getErrorMessage())
          : "DataHub error %s: %s".formatted(de.getErrorCode(), de.getErrorMessage());
    }
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }

  public static RuntimeException translate(Throwable e) {
    if (e instanceof ResourceNotFoundException nf) {
      return "NoSuchTopic".equals(nf.getErrorCode())
          ? new TopicNotFoundException()
          : new NotFoundException(describe(e));
    }
    if (e instanceof DatahubClientException) {
      return new ValidationException(describe(e));
    }
    return e instanceof RuntimeException re ? re : new IllegalStateException(e);
  }
}
