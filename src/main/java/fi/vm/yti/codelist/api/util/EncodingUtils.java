package fi.vm.yti.codelist.api.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.springframework.http.HttpStatus;

import fi.vm.yti.codelist.api.exception.YtiCodeListException;
import fi.vm.yti.codelist.common.dto.ErrorModel;
import static fi.vm.yti.codelist.api.exception.ErrorConstants.ERR_MSG_USER_406;
import static fi.vm.yti.codelist.api.exception.ErrorConstants.ERR_MSG_USER_ERROR_ENCODING_STRING;

public interface EncodingUtils {

    static String urlDecodeString(final String string) {
        try {
            return URLDecoder.decode(string, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), ERR_MSG_USER_406));
        }
    }

    static String urlDecodeCodeValue(final String string) {
        if (string.equalsIgnoreCase("U+002E")) {
            return ".";
        } else if (string.equalsIgnoreCase("U+002EU+002E")) {
            return "..";
        }
        return urlDecodeString(string);
    }

    static String urlEncodeCodeValue(final String codeValue) {
        try {
            final String codeValueToBeEncoded;
            switch (codeValue) {
                case ".":
                    codeValueToBeEncoded = "U+002E";
                    break;
                case "..":
                    codeValueToBeEncoded = "U+002EU+002E";
                    break;
                default:
                    codeValueToBeEncoded = codeValue;
                    break;
            }
            return URLEncoder.encode(codeValueToBeEncoded, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), ERR_MSG_USER_ERROR_ENCODING_STRING));
        }
    }
}
