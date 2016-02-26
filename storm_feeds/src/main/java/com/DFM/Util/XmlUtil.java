package com.DFM.Util;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;

@SuppressWarnings("ThrowFromFinallyBlock")
public class XmlUtil {
    public static Document deserialize(String sXML) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(sXML)));
    }

    public static Document loadXMLFrom(String xml) throws Exception {
        return loadXMLFrom(new ByteArrayInputStream(xml.getBytes()));
    }

    public static Document loadXMLFrom(InputStream inputStream) throws Exception {
        Document doc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        javax.xml.parsers.DocumentBuilder builder = null;
        try {
            builder = factory.newDocumentBuilder();
            doc = builder.parse(inputStream);
        } catch (ParserConfigurationException pce) {
            throw new Exception(pce.getMessage());
        } catch (SAXException se) {
            throw new Exception(se.getMessage());
        } catch (IOException ioe) {
            throw new Exception(ioe.getMessage());
        } finally {
            inputStream.close();
        }
        return doc;
    }

    public static Document deserialize(Object object) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.newDocument();

        JAXBContext context = JAXBContext.newInstance(object.getClass());
        Marshaller m = context.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        m.marshal(object, doc);
        return doc;
    }

    public static String XMLtoString(Node node) throws Exception {
        Document document = node.getOwnerDocument();
        DOMImplementationLS domImplLS = (DOMImplementationLS) document.getImplementation();
        LSSerializer serializer = domImplLS.createLSSerializer();
        serializer.getDomConfig().setParameter("xml-declaration", false);
        return serializer.writeToString(node);
    }

    public static String XMLtoString(Document doc) throws Exception {
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer;
            transformer = tf.newTransformer();
            // below code to remove XML declaration
            // transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            return writer.getBuffer().toString();
        } catch (TransformerException te) {
            throw new Exception(te.getMessageAndLocation());
        }
    }

    public static String transform(String xmlString, String stylesheetPathname) throws Exception {
        try {
            TransformerFactory factory = TransformerFactory.newInstance();
            Source stylesheetSource = new StreamSource(new File(stylesheetPathname).getAbsoluteFile());
            Transformer transformer = factory.newTransformer(stylesheetSource);
            Source inputSource = new StreamSource(new StringReader(xmlString));
            Writer outputWriter = new StringWriter();
            Result outputResult = new StreamResult(outputWriter);
            transformer.transform(inputSource, outputResult);
            return outputWriter.toString();
        } catch (TransformerConfigurationException tce) {
            throw new Exception(tce.getMessageAndLocation());
        } catch (TransformerException te) {
            throw new Exception(te.getMessageAndLocation());
        }
    }

    public static String getNodeValue(String xml, String nodeName) throws Exception {
        Document doc = loadXMLFrom(xml);
        return getFirstChildNodeValue(doc, nodeName);
    }

    public static String getAttributeValue(String xml, String nodeName, String attributeName) throws Exception {
        Document doc = loadXMLFrom(xml);
        return getFirstChildNodeAttributeValue(doc, nodeName, attributeName);
    }

    public static String getFirstChildNodeAttributeValue(Element parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }

    public static String getFirstChildNodeValue(Document parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }

    public static String getFirstChildNodeAttributeValue(Document parent, String nodeName, String attributeName) {
        return parent.getElementsByTagName(nodeName).item(0).getAttributes().getNamedItem(attributeName).getNodeValue();
    }

    public static String getFirstValueFromXPath(Element parent, String xpath) throws Exception {
        try {
            XPath xPath = XPathFactory.newInstance().newXPath();
            return (String) xPath.compile(xpath).evaluate(parent, XPathConstants.STRING);
        } catch (XPathExpressionException xpee) {
            throw new Exception(xpee.getMessage());
        }
    }

    public static String getFirstValueFromXPath(Document parent, String xpath) throws Exception {
        try {
            XPath xPath = XPathFactory.newInstance().newXPath();
            return (String) xPath.compile(xpath).evaluate(parent, XPathConstants.STRING);
        } catch (XPathExpressionException xpee) {
            throw new Exception(xpee.getMessage());
        }
    }

    public String getValue(Element parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }
}

