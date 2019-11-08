package fi.vm.yti.codelist.api.exception.exceptionmapping;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.vm.yti.codelist.api.exception.YtiCodeListException;

@Provider
public class YtiCodeListExceptionMapper implements BaseExceptionMapper, ExceptionMapper<YtiCodeListException> {

    private static final Logger LOG = LoggerFactory.getLogger(UncaughtExceptionMapper.class);

    @Override
    public Response toResponse(final YtiCodeListException ex) {
        LOG.error("YtiCodelistException occurred: ", ex);
        return getResponse(ex);
    }
}
