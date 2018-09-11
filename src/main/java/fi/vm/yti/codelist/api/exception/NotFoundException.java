package fi.vm.yti.codelist.api.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import fi.vm.yti.codelist.common.dto.ErrorModel;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class NotFoundException extends YtiCodeListException {

    private static final String RESOURCE_NOT_FOUND = "Resource not found!";

    public NotFoundException() {
        super(new ErrorModel(HttpStatus.NOT_FOUND.value(), RESOURCE_NOT_FOUND));
    }
}
