package com.ljg.panda.common.text;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/**
 * Text and parsing related utility methods.
 * 文本解析相关的工具方法
 */
public final class TextUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final CSVFormat CSV_FORMAT =
            CSVFormat.RFC4180.withSkipHeaderRecord().withEscape('\\');
    private static final String[] EMPTY_STRING = {""};
    private static final Pattern TWO_DOUBLE_QUOTE_ESC = Pattern.compile("\"\"", Pattern.LITERAL);
    private static final String SLASH_QUOTE_ESC = Matcher.quoteReplacement("\\\"");

    private TextUtils() {
    }

    // Delimited --

    /**
     * 按照指定的格式解析文本的每一行数据
     * @param delimited line of delimited text
     * @param delimiter delimiter to split fields on
     * @return delimited strings, parsed according to RFC 4180 but with the given delimiter
     */
    public static String[] parseDelimited(String delimited, char delimiter) {
        return doParseDelimited(delimited, formatForDelimiter(delimiter));
    }

    /**
     * 解析以空格隔开的PMML字符串
     * @param delimited PMML-style space-delimited value string
     * @return delimited values, parsed according to PMML rules
     */
    public static String[] parsePMMLDelimited(String delimited) {
        // Although you'd think ignoreSurroundingSpaces helps here, won't work with space
        // delimiter. So manually trim below.
        String[] rawResult = doParseDelimited(delimited, formatForDelimiter(' '));
        List<String> resultList = new ArrayList<>();
        for (String raw : rawResult) {
            if (!raw.isEmpty()) {
                resultList.add(raw);
            }
        }
        return resultList.toArray(new String[resultList.size()]);
    }

    /**
     *  使用指定的CSV格式解析字符串，并返回字符串数组
     * @param delimited
     * @param format
     * @return
     */
    private static String[] doParseDelimited(String delimited, CSVFormat format) {
        try (CSVParser parser = CSVParser.parse(delimited, format)) {
            Iterator<CSVRecord> records = parser.iterator();
            return records.hasNext() ?
                    StreamSupport.stream(records.next().spliterator(), false).toArray(String[]::new) :
                    EMPTY_STRING;
        } catch (IOException e) {
            throw new IllegalStateException(e); // Can't happen
        }
    }

    /**
     *  将指定的集合中的元素按照指定的分隔符拼接起来
     * @param elements  values to join by the delimiter to make one line of text
     * @param delimiter delimiter to put between fields
     * @return one line of text, with RFC 4180 escaping (values with comma are quoted; double-quotes
     * are escaped by doubling) and using the given delimiter
     */
    public static String joinDelimited(Iterable<?> elements, char delimiter) {
        return doJoinDelimited(elements, formatForDelimiter(delimiter));
    }

    /**
     *  将指定的集合中的元素按照空格分隔符拼接起来
     * @param elements values to join by space to make one line of text
     * @return one line of text, formatted according to PMML quoting rules
     * (\" instead of "" for escaping quotes; ignore space surrounding values
     */
    public static String joinPMMLDelimited(Iterable<?> elements) {
        String rawResult = doJoinDelimited(elements, formatForDelimiter(' '));
        // Must change "" into \"
        return TWO_DOUBLE_QUOTE_ESC.matcher(rawResult).replaceAll(SLASH_QUOTE_ESC);
    }

    /**
     *  将指定的Number类型集合中的元素按照空格分隔符拼接起来
     * @param elements numbers to join by space to make one line of text
     * @return one line of text, formatted according to PMML quoting rules
     */
    public static String joinPMMLDelimitedNumbers(Iterable<? extends Number> elements) {
        // bit of a workaround because NON_NUMERIC quote mode still quote "-1"!
        CSVFormat format = formatForDelimiter(' ').withQuoteMode(QuoteMode.NONE);
        // No quoting, no need to convert quoting
        return doJoinDelimited(elements, format);
    }

    /**
     *  返回指定的分隔符的CSVFormat
     * @param delimiter
     * @return
     */
    private static CSVFormat formatForDelimiter(char delimiter) {
        CSVFormat format = CSV_FORMAT;
        if (delimiter != format.getDelimiter()) {
            format = format.withDelimiter(delimiter);
        }
        return format;
    }

    /**
     *  使用指定的CSV格式拼接字符串集合，并返回拼接后的字符串
     * @param elements
     * @param format
     * @return
     */
    private static String doJoinDelimited(Iterable<?> elements, CSVFormat format) {
        StringWriter out = new StringWriter();
        try (CSVPrinter printer = new CSVPrinter(out, format)) {
            for (Object element : elements) {
                printer.print(element);
            }
            printer.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toString();
    }

    /// JSON --

    /**
     * 解析数组类型的Json
     * @param json line of JSON text
     * @return delimited strings, the elements of the JSON array
     * @throws IOException if JSON parsing fails
     */
    public static String[] parseJSONArray(String json) throws IOException {
        return MAPPER.readValue(json, String[].class);
    }

    /**
     * 将集合解析成Json
     * @param elements elements to be joined in a JSON string. May be any objects.
     * @return JSON representation of the list of objects
     */
    public static String joinJSON(Iterable<?> elements) {
        try {
            return MAPPER.writeValueAsString(elements);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 将String类型的Json转成指定类型的对象
     * @param json  JSON string
     * @param clazz Java type to interpret as
     * @param <T>   type that should be parsed from JSON and returned
     * @return the JSON string, parsed into the given type
     */
    public static <T> T readJSON(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 利用Json解析器将Object转成指定类型的对象
     * @param value value to convert
     * @param clazz desired type to interpret as
     * @param <T>   type that should be parsed from JSON and returned
     * @return the given value, reinterpreted as the given type, as if serialized/deserialized
     * via JSON to perform the conversion
     */
    public static <T> T convertViaJSON(Object value, Class<T> clazz) {
        return MAPPER.convertValue(value, clazz);
    }

}
