package io.primeval.json.jackson;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.primeval.common.type.TypeTag;
import io.primeval.json.JsonSerDes;

public interface JacksonMapper extends JsonSerDes {
    String JACKSON_MODULE_PROPERTY = "jackson.module";

    ObjectMapper objectMapper();

    default JavaType toJavaType(TypeTag<?> typeTag) {
        return objectMapper().getTypeFactory().constructType(typeTag.type);
    }
}
