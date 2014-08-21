package nl.utwente.db.neogeo.preaggregate;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class PreAggregateConfig {
    static final Logger logger = Logger.getLogger(PreAggregateConfig.class);
    
    protected String aggregateType;
    
    protected int aggregateMask;
    
    protected List<AggregateAxis> axisList;
    
    public PreAggregateConfig (File xmlFile) throws InvalidConfigException {
        axisList = new ArrayList<AggregateAxis>();
        loadFromFile(xmlFile);
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
        
    protected void handleMask(Node node) {
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
                logger.warn("Invalid aggregate mask specified in config file");
            }
        }
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
