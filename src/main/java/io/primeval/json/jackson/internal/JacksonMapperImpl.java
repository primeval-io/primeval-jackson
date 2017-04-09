package io.primeval.json.jackson.internal;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.primeval.common.property.PropertyHelper;
import io.primeval.common.serdes.DeserializationException;
import io.primeval.common.serdes.SerializationException;
import io.primeval.common.type.TypeTag;
import io.primeval.json.JsonConstants;
import io.primeval.json.JsonDeserializer;
import io.primeval.json.JsonSerDes;
import io.primeval.json.JsonSerializer;
import io.primeval.json.jackson.JacksonMapper;

@Component(immediate = true, service = { JacksonMapper.class, JsonSerDes.class, JsonSerializer.class,
        JsonDeserializer.class })
public final class JacksonMapperImpl implements JacksonMapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(JacksonMapperImpl.class);

    private final List<Module> BASE_MODULES = Arrays.asList(new Jdk8Module(), new JavaTimeModule(),
            new ParameterNamesModule());

    private final Object lock = new Object();
    private ObjectMapper mapper;
    private List<Module> modules = new CopyOnWriteArrayList<>();
    private Set<String> providedProperties = new ConcurrentSkipListSet<>();
    private ConfigurationAdmin configurationAdmin;

    @Reference
    public void setConfigurationAdmin(ConfigurationAdmin configurationAdmin) {
        this.configurationAdmin = configurationAdmin;
    }

    @Activate
    public void activate() {
        synchronized (lock) {
            rebuildMappers();
        }
    }

    @Modified
    public void modified() {
        // leave empty to declare we support dynamic configuration updates
        // otherwise shit happens
    }

    public void ensureMapperConfig() {
        mapper = new ObjectMapper();
        this.mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        this.mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

        // TODO Make Filters & AnnotationIntrospecter settable with service
    }

    @Reference(policy = ReferencePolicy.DYNAMIC, cardinality = ReferenceCardinality.MULTIPLE)
    public void addJacksonModule(Module module, Map<String, Object> properties) {
        if (module == null) {
            return;
        }

        synchronized (lock) {
            modules.add(module);
            if (properties != null) {
                String[] jsonModules = PropertyHelper.getProperty(JACKSON_MODULE_PROPERTY, String.class, properties);
                if (jsonModules != null) {
                    Stream.of(jsonModules).forEach(prop -> providedProperties.add(prop));
                    updateConfiguration();
                }
            }
            rebuildMappers();
        }
    }

    public void removeJacksonModule(Module module, Map<String, Object> properties) {
        if (module == null) {
            // May happen on departure.
            return;
        }
        synchronized (lock) {
            if (modules.remove(module)) {
                if (properties != null) {
                    String[] jsonModules = PropertyHelper.getProperty(JACKSON_MODULE_PROPERTY, String.class,
                            properties);
                    if (jsonModules != null) {
                        Stream.of(jsonModules).forEach(prop -> providedProperties.remove(prop));
                        updateConfiguration();
                    }
                }
                rebuildMappers();
            }
        }
    }

    private void updateConfiguration() {
        try {
            org.osgi.service.cm.Configuration componentConfig = configurationAdmin
                    .getConfiguration(JacksonMapperImpl.class.getName(), null);
            Dictionary<String, Object> properties = componentConfig.getProperties();
            if (properties == null) {
                properties = new Hashtable<>();
            }
            properties.put(JACKSON_MODULE_PROPERTY, providedProperties.toArray(new String[0]));
            componentConfig.update(properties);
        } catch (IOException e) {
            LOGGER.error("Could not get configuration: things might not work so well...", e);
        }

    }

    private void rebuildMappers() {
        ensureMapperConfig();
        for (Module m : BASE_MODULES) {
            mapper.registerModule(m);
        }
        for (Module module : modules) {
            mapper.registerModule(module);
        }
        // applyMapperConfiguration(mapper);
    }

    @Override
    public <T> String toString(T object, TypeTag<? extends T> typeTag) {
        try {
            return mapper.writerFor(toJavaType(typeTag)).writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException(typeTag, JsonConstants.MEDIATYPE, e);
        }
    }

    @Override
    public <T> byte[] toByteArray(T object, TypeTag<? extends T> typeTag) {
        try {
            return mapper.writerFor(toJavaType(typeTag)).writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException(typeTag, JsonConstants.MEDIATYPE, e);
        }
    }

    @Override
    public <T> T fromJson(CharSequence source, TypeTag<T> typeTag, ClassLoader classLoader) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return mapper.readerFor(toJavaType(typeTag)).readValue(source.toString());
        } catch (IOException e) {
            throw new DeserializationException(typeTag, e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }

    }

    @Override
    public <T> T fromJson(InputStream source, TypeTag<T> typeTag, ClassLoader classLoader) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return mapper.readerFor(toJavaType(typeTag)).readValue(source);
        } catch (IOException e) {
            throw new DeserializationException(typeTag, e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    @Override
    public ObjectMapper objectMapper() {
        return mapper;
    }

    public void addModule(Module module) {
        addJacksonModule(module, null);
    }

    public void removeModule(Module module) {
        removeJacksonModule(module, null);
    }

}
