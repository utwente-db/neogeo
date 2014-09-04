package nl.utwente.db.neogeo.preaggregate;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class PreAggregateConfig {
    static final Logger logger = Logger.getLogger(PreAggregateConfig.class);
    
    protected String table;
    
    protected String column;
    
    protected String label;
    
    protected String aggregateType;
    
    protected int aggregateMask;
    
    protected List<AggregateAxis> axisList;
    
    protected char keyKind = PreAggregate.DEFAULT_KD;
    
    public PreAggregateConfig(String table, String column, String label, String aggregateType, int aggregateMask, AggregateAxis[] axis) {
        this.table = table;
        this.column = column;
        this.label = label;
        
        if (aggregateType.equalsIgnoreCase("int")) aggregateType = "integer";
        this.aggregateType = aggregateType;
        
        this.aggregateMask = aggregateMask;
                
        this.axisList = new ArrayList<AggregateAxis>();        
        for(int i=0; i < axis.length; i++) {
            this.axisList.add(axis[i]);
        }
    }
        
    public PreAggregateConfig (File xmlFile) throws InvalidConfigException {
        axisList = new ArrayList<AggregateAxis>();
        loadFromFile(xmlFile);
    }
    
    public void writeToXml(File xmlFile) throws ParserConfigurationException, TransformerConfigurationException, TransformerException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
	DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        
        // root elements
        Document doc = docBuilder.newDocument();
        Element rootElement = doc.createElement("preaggregate");
        doc.appendChild(rootElement);
        
        if (table != null && table.isEmpty() == false) {
            Element tableEl = doc.createElement("table");
            tableEl.setTextContent(this.table);
            rootElement.appendChild(tableEl);
        }
        
        if (column != null && column.isEmpty() == false) {
            Element columnEl = doc.createElement("column");
            columnEl.setTextContent(this.column);
            rootElement.appendChild(columnEl);
        }
        
        if (label != null && label.isEmpty() == false) {
            Element labelEl = doc.createElement("label");
            labelEl.setTextContent(this.label);
            rootElement.appendChild(labelEl);
        }
        
        if (aggregateType != null && aggregateType.isEmpty() == false) {
            Element aggregateTypeEl = doc.createElement("type");
            aggregateTypeEl.setTextContent(aggregateType);
            rootElement.appendChild(aggregateTypeEl);
        }
        
        if (aggregateMask != -1) {
            Element aggregateMaskEl = doc.createElement("mask");
            aggregateMaskEl.setTextContent(String.valueOf(this.aggregateMask));
            rootElement.appendChild(aggregateMaskEl);
        }
        
        Element keyKindEl = doc.createElement("keykind");
        keyKindEl.setTextContent(String.valueOf(this.keyKind));
        rootElement.appendChild(keyKindEl);
        
        if (axisList.size() > 0) {
            Element axisListEl = doc.createElement("axislist");
            rootElement.appendChild(axisListEl);
            
            for(AggregateAxis axis : axisList) {
                Element axisEl = doc.createElement("axis");

                axisEl.setAttribute("class", axis.getClass().getSimpleName());                
                axisEl.setAttribute("column", axis.columnExpression());
                axisEl.setAttribute("type", axis.sqlType());
                axisEl.setAttribute("n", String.valueOf(axis.N()));
                
                if (axis instanceof MetricAxis) {
                    MetricAxis metricAxis = (MetricAxis) axis;
                    axisEl.setAttribute("baseblocksize", String.valueOf(metricAxis.BASEBLOCKSIZE()));
                    axisEl.setAttribute("low", String.valueOf(metricAxis.low()));
                    axisEl.setAttribute("high", String.valueOf(metricAxis.high()));
                }
                
                axisListEl.appendChild(axisEl);
            }
        }
        
        // write the content into xml file
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(xmlFile);

        transformer.transform(source, result);
    }
    
    public String getAggregateType () {
        return this.aggregateType;
    }
    
    public int getAggregateMask () {
        return this.aggregateMask;
    }
    
    public List<AggregateAxis> getAxisList () {
        return this.axisList;
    }
    
    public char getKeyKind () {
        return this.keyKind;
    }
    
    public void setKeyKind (char keyKind) {
        this.keyKind = keyKind;
    }
    
    public AggregateAxis[] getAxis () {
        return axisList.toArray(new AggregateAxis[axisList.size()]);
    }
    
    protected void loadFromFile (File xmlFile) throws InvalidConfigException {
        try {
            if (xmlFile.exists() == false) {
                throw new InvalidConfigException("XML file does not exist");
            }
            
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            
            //optional, but recommended
            //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
            doc.getDocumentElement().normalize();
            
            NodeList children = doc.getDocumentElement().getChildNodes();
            for(int i=0; i < children.getLength(); i++) {
                handleNode(children.item(i));
            }
        } catch (InvalidConfigException ex) {
            // directly forward our own exceptions
            throw ex;
        } catch (Exception ex) {
            throw new InvalidConfigException("Unable to parse XML file", ex);
        }
    }
    
    protected void handleNode(Node node) throws InvalidConfigException {
        String nodeName = node.getNodeName().toLowerCase();
        
        if (nodeName.equals("type")) {
            aggregateType = node.getTextContent();
            if (aggregateType.equalsIgnoreCase("int")) aggregateType = "integer";
        } else if (nodeName.equals("table")) {
            table = node.getTextContent();
        } else if (nodeName.equals("column")) {
            column = node.getTextContent();
        } else if (nodeName.equals("label")) {
            label = node.getTextContent();
        } else if (nodeName.equals("keykind")) {
            handleKeyKind(node);
        } else if (nodeName.equals("mask")) {
            handleMask(node);            
        } else if (nodeName.equals("axislist")) {
            NodeList children = node.getChildNodes();
            for(int i=0; i < children.getLength(); i++) {
                handleNode(children.item(i));
            }
        } else if (nodeName.equals("axis")) {
            handleAxis(node);
        } else if (node.getNodeType() == Node.TEXT_NODE) {
            // ignore text nodes
        } else {
            throw new InvalidConfigException("Unknown node '" + nodeName + "' in config file");
        }
    }
    
    protected void handleAxis (Node node) throws InvalidConfigException {
        NamedNodeMap attr = node.getAttributes();
        
        Node classNode = attr.getNamedItem("class");
        if (classNode == null) {
            logger.warn("Invalid axis node detected: missing 'class' attribute");
            return;
        }
        
        Node columnNode = attr.getNamedItem("column");
        if (columnNode == null) {
            logger.warn("Invalid axis node detected: missing 'column' attribute");
            return;
        }
        
        String className = classNode.getTextContent();
        String column = columnNode.getTextContent();
                
        if (column.isEmpty()) {
            throw new InvalidConfigException("Invalid axis node detected: column attribute cannot be empty");
        }
        
        if (className.equalsIgnoreCase("MetricAxis")) {
            handleMetricAxis(node, attr, column);
        } else {
            throw new InvalidConfigException("Invalid axis node detected: axis of class type '" + className + "' not yet supported!");
        }         
    }
    
    protected void handleMetricAxis (Node node, NamedNodeMap attr, String column) throws InvalidConfigException {
        Node typeNode = attr.getNamedItem("type");
        if (typeNode == null) {
            throw new InvalidConfigException("Invalid axis node detected: missing 'type' attribute");
        }
        
        Node baseblocksizeNode = attr.getNamedItem("baseblocksize");
        if (baseblocksizeNode == null) {
            throw new InvalidConfigException("Invalid axis node detected: missing 'baseblocksize' attribute");

        }
        
        Node nNode = attr.getNamedItem("n");
        if (nNode == null) {
            throw new InvalidConfigException("Invalid axis node detected: missing 'n' attribute");
        }

        String type = typeNode.getTextContent();
        String baseblocksize = baseblocksizeNode.getTextContent();
        String nStr = nNode.getTextContent();
        
        if (type.isEmpty()) {
            throw new InvalidConfigException("Invalid axis node detected: 'type' attribute cannot be empty");
        }
        
        if (baseblocksize.isEmpty()) {
            throw new InvalidConfigException("Invalid axis node detected: 'baseblocksize' attribute cannot be empty");
        }
        
        short N;
        try {
            N = Short.parseShort(nStr);
        } catch (NumberFormatException e) {
            throw new InvalidConfigException("Invalid axis node detected: 'n' attribute must be a valid short");
        }
        
        Node lowNode = attr.getNamedItem("low");
        Node highNode = attr.getNamedItem("high");
        
        String low = null;
        String high = null;
        if (lowNode != null && highNode != null) {
            low = lowNode.getTextContent();
            high = highNode.getTextContent();
        }
        
        
        MetricAxis axis = new MetricAxis(column, type, baseblocksize, N);
        
        if (low != null && low.isEmpty() == false && high != null && high.isEmpty() == false) {
            axis.setRangeValues(low, high);
        }
        
        this.axisList.add(axis);
    }
        
    protected void handleKeyKind (Node node) throws InvalidConfigException {
        String val = node.getTextContent();
        
        if (val.length() > 1) {
            throw new InvalidConfigException("Invalid KeyKind specified in config file");
        }
        
        // get KeyKind
        keyKind = val.charAt(0);
    }
    
    protected void handleMask(Node node) throws InvalidConfigException {
        String mask = node.getTextContent();
            
        if (mask.equalsIgnoreCase("ALL")) {
            this.aggregateMask = PreAggregate.AGGR_ALL;
        } else if (mask.equalsIgnoreCase("COUNT")) {
            this.aggregateMask = PreAggregate.AGGR_COUNT;
        } else if (mask.equalsIgnoreCase("SUM")) {
            this.aggregateMask = PreAggregate.AGGR_SUM;
        } else if (mask.equalsIgnoreCase("MIN")) {
            this.aggregateMask = PreAggregate.AGGR_MIN;
        } else if (mask.equalsIgnoreCase("MAX")) {
            this.aggregateMask = PreAggregate.AGGR_MAX;
        } else {
            // try to parse mask as an integer
            try {
                this.aggregateMask = Integer.parseInt(mask);
            } catch (NumberFormatException e) {
                throw new InvalidConfigException("Invalid aggregate mask specified in config file");
            }
        }
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }
    
    public String getAggregateColumn() {
        return column;
    }

    public String getLabel() {
        return label;
    }
    
    public class InvalidConfigException extends Exception {
        public InvalidConfigException (String msg) {
            super(msg);
        }
        
        public InvalidConfigException (String msg, Exception originalException) {
            super(msg, originalException);
        }
    }
    
}
