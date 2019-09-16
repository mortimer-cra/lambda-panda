package com.ljg.panda.common.pmml;

import org.dmg.pmml.*;
import org.dmg.pmml.tree.Node;
import org.dmg.pmml.tree.TreeModel;
import org.jpmml.model.JAXBUtil;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


public final class PMMLUtilsTest {

    public static final String VERSION = "4.3";

    @Test
    public void testPMML() {
        String formattedDate =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ", Locale.ENGLISH).format(new Date());
        // Header表示PMML的一些通用的说明
        Header header = new Header()
                .setTimestamp(new Timestamp().addContent(formattedDate)) // PMML模型创建的时间
                .setApplication(new Application("Panda")) // 模型名称
                .setCopyright("版权说明")
                .setModelVersion("模型的版本")
                .setDescription("模型的描述");
        // DataDictionary 包含了模型需要的参数
        DataDictionary dataDictionary = new DataDictionary();
        DataField dataField = new DataField().setDataType(DataType.DOUBLE)
                .setName(new FieldName("test"))
                .setOpType(OpType.CONTINUOUS)
                .addValues(new Value().setValue("1.200"))
                .addValues(new Value().setValue("1.500"));
        dataDictionary.addDataFields(dataField);
        PMML pmml = new PMML(VERSION, header, dataDictionary);
        // Model就是这个PMML承载的Model信息
        Node node = new Node().setRecordCount(123.0);
        TreeModel treeModel = new TreeModel(MiningFunction.CLASSIFICATION, null, node);
        pmml.addModels(treeModel);
        // Extensions 为扩展字段信息
        pmml.addExtensions(new Extension().setName("test").setValue("value1"));

        // 将对象pmml转化成字符串
        String pmmlStr = null;
        try (StringWriter out = new StringWriter()) {
            // v JAXBUtil.marshalPMML but need to set compact, non-pretty output
            Marshaller marshaller = JAXBUtil.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.FALSE);
            marshaller.marshal(pmml, new StreamResult(out));
            pmmlStr = out.toString();
        } catch (JAXBException | IOException e) {
            // IOException should not be possible; JAXBException would only happen with XML
            // config problems.
            throw new IllegalStateException(e);
        }
        System.out.println("pmml : " + pmmlStr);
    }

}
