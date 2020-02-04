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

    String UTF_8_ENCODING = "UTF-8";
    String DOT_VALUE_ENCODED = "U+002E";
    String DOT_VALUE_ENCODED_VARIANT = "U%2B002E";
    String DOT_VALUE_DECODED = ".";
    String DOT_DOT_VALUE_DECODED = "..";
    String DOT_DOT_VALUE_ENCODED = "U+002EU+002E";
    String DOT_DOT_VALUE_ENCODED_VARIANT = "U%2B002EU%2B002E";

    static String urlDecodeString(final String string) {
        try {
            return URLDecoder.decode(string, UTF_8_ENCODING);
        } catch (final UnsupportedEncodingException e) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), ERR_MSG_USER_406));
        }
    }

    static String decodeDotCodeValues(final String codeValue) {
        if (codeValue.equalsIgnoreCase(DOT_VALUE_ENCODED)) {
            return DOT_VALUE_DECODED;
        } else if (codeValue.equalsIgnoreCase(DOT_DOT_VALUE_ENCODED)) {
            return DOT_DOT_VALUE_DECODED;
        }
        return codeValue;
    }

    static String urlDecodeCodeValue(final String string) {
        if (string.equalsIgnoreCase(DOT_VALUE_ENCODED) || string.equalsIgnoreCase(DOT_VALUE_ENCODED_VARIANT)) {
            return DOT_VALUE_DECODED;
        } else if (string.equalsIgnoreCase(DOT_DOT_VALUE_ENCODED) || string.equalsIgnoreCase(DOT_DOT_VALUE_ENCODED_VARIANT)) {
            return DOT_DOT_VALUE_DECODED;
        }
        return urlDecodeString(string);
    }

    static String urlEncodeCodeValue(final String codeValue) {
        try {
            final String codeValueToBeEncoded;
            switch (codeValue) {
                case DOT_VALUE_DECODED:
                    codeValueToBeEncoded = DOT_VALUE_ENCODED;
                    break;
                case DOT_DOT_VALUE_DECODED:
                    codeValueToBeEncoded = DOT_DOT_VALUE_ENCODED;
                    break;
                default:
                    codeValueToBeEncoded = codeValue;
                    break;
            }
            return URLEncoder.encode(codeValueToBeEncoded, UTF_8_ENCODING);
        } catch (final UnsupportedEncodingException e) {
            throw new YtiCodeListException(new ErrorModel(HttpStatus.NOT_ACCEPTABLE.value(), ERR_MSG_USER_ERROR_ENCODING_STRING));
        }
    }
}
