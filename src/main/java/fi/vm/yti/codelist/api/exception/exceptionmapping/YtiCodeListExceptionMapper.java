package fi.vm.yti.codelist.api.exception.exceptionmapping;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import fi.vm.yti.codelist.api.exception.YtiCodeListException;

@Provider
public class YtiCodeListExceptionMapper implements BaseExceptionMapper, ExceptionMapper<YtiCodeListException> {

    @Override
    public Response toResponse(final YtiCodeListException ex) {
        return getResponse(ex);
    }
}
