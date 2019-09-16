package com.ljg.panda.common.pmml;

import org.dmg.pmml.Application;
import org.dmg.pmml.Header;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Timestamp;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.jpmml.model.PMMLUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * PMML全称预测模型标记语言（Predictive Model Markup Language），
 * 以XML为载体呈现数据挖掘模型。PMML 允许你在不同的应用程序之间轻松共享预测分析模型。
 * 因此，你可以在一个系统中定型一个模型，在 PMML 中对其进行表达，
 * 然后将其移动到另一个系统中，而不需考虑分析和预测过程中的具体实现细节。
 * 使得模型的部署摆脱了模型开发和产品整合的束缚。
 *
 * PMML 允许你在不同的应用程序之间轻松共享预测分析模型。
 * 因此，你可以在一个系统中定义一个模型，使用 PMML 对其进行表达，然后将其移动到另一个系统中
 *
 * 参考：https://en.wikipedia.org/wiki/Predictive_Model_Markup_Language
 */
public class PMMLUtils {
    public static final String VERSION = "4.3";

    private PMMLUtils() {}

    /**
     * 创建一个PMML
     * @return {@link PMML} with common {@link Header} fields like {@link Application},
     *  {@link Timestamp}, and version filled out
     */
    public static PMML buildSkeletonPMML() {
        String formattedDate =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ", Locale.ENGLISH).format(new Date());
        Header header = new Header()
                .setTimestamp(new Timestamp().addContent(formattedDate))
                .setApplication(new Application("Panda"));
        return new PMML(VERSION, header, null);
    }

    /**
     * 保存PMML到一个文件中
     * @param pmml {@link PMML} model to write
     * @param path file to write the model to
     * @throws IOException if an error occurs while writing the model to storage
     */
    public static void write(PMML pmml, Path path) throws IOException {
        try (OutputStream out = Files.newOutputStream(path)) {
            write(pmml, out);
        }
    }

    /**
     * 将PMML写到一个输出流中
     * @param pmml {@link PMML} model to write
     * @param out stream to write the model to
     * @throws IOException if an error occurs while writing the model to storage
     */
    public static void write(PMML pmml, OutputStream out) throws IOException {
        try {
            PMMLUtil.marshal(pmml, out);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
    }

    /**
     * 从一个文件中读出PMML
     * @param path file to read PMML from
     * @return {@link PMML} model file from path
     * @throws IOException if an error occurs while reading the model from storage
     */
    public static PMML read(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return read(in);
        }
    }

    /**
     * 从一个输入流中读出PMML
     * @param in stream to read PMML from
     * @return {@link PMML} model file from stream
     * @throws IOException if an error occurs while reading the model from the stream
     */
    public static PMML read(InputStream in) throws IOException {
        try {
            return PMMLUtil.unmarshal(in);
        } catch (JAXBException | SAXException e) {
            throw new IOException(e);
        }
    }

    /**
     * 将PMML转成String类型
     * @param pmml model
     * @return model serialized as an XML document as a string
     */
    public static String toString(PMML pmml) {
        try (StringWriter out = new StringWriter()) {
            // v JAXBUtil.marshalPMML but need to set compact, non-pretty output
            Marshaller marshaller = JAXBUtil.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.FALSE);
            marshaller.marshal(pmml, new StreamResult(out));
            return out.toString();
        } catch (JAXBException | IOException e) {
            // IOException should not be possible; JAXBException would only happen with XML
            // config problems.
            throw new IllegalStateException(e);
        }
    }

    /**
     * 将String类型转成PMML
     * @param pmmlString PMML model encoded as an XML doc in a string
     * @return {@link PMML} object representing the model
     * @throws IOException if XML can't be unserialized
     */
    public static PMML fromString(String pmmlString) throws IOException {
        // Emulate PMMLUtil.unmarshal here, but need to accept a Reader
        InputSource source = new InputSource(new StringReader(pmmlString));
        try {
            return JAXBUtil.unmarshalPMML(ImportFilter.apply(source));
        } catch (JAXBException | SAXException e) {
            throw new IOException(e);
        }
    }
}
