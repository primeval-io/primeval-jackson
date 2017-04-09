package io.primeval.json.jackson.test.rules;

import java.util.List;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.fasterxml.jackson.databind.Module;

import io.primeval.common.test.rules.TestResource;
import io.primeval.json.jackson.internal.JacksonMapperImpl;

public final class WithJacksonMapper extends ExternalResource implements TestResource {

    private JacksonMapperImpl jacksonMapper;
    private List<Supplier<Module>> moduleSuppliers;

    public WithJacksonMapper(List<Supplier<Module>> moduleSuppliers) {
        this.moduleSuppliers = moduleSuppliers;
    }

    public JacksonMapperImpl getJacksonMapper() {
        return jacksonMapper;
    }

    @Override
    public void before() throws Throwable {
        JacksonMapperImpl jacksonMapper = new JacksonMapperImpl();
        jacksonMapper.activate();

        moduleSuppliers.stream().map(Supplier::get).forEach(jacksonMapper::addModule);

        this.jacksonMapper = jacksonMapper;
    }

    @Override
    public void after() {
    }
}
