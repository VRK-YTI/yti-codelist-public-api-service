package fi.vm.yti.codelist.api.exception.exceptionmapping;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.springframework.http.HttpStatus;

import fi.vm.yti.codelist.api.api.ResponseWrapper;
import fi.vm.yti.codelist.api.exception.ErrorConstants;
import fi.vm.yti.codelist.common.dto.Meta;

@Provider
public class UncaughtExceptionMapper implements ExceptionMapper<Exception> {

    @Override
    public Response toResponse(final Exception ex) {
        final ResponseWrapper wrapper = new ResponseWrapper();
        final Meta meta = new Meta();
        meta.setMessage(ErrorConstants.ERR_MSG_USER_500);
        meta.setCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
        wrapper.setMeta(meta);
        return Response.status(HttpStatus.INTERNAL_SERVER_ERROR.value()).entity(wrapper).build();
    }
}
