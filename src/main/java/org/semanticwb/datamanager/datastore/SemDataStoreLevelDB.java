/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.semanticwb.datamanager.datastore;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bson.types.ObjectId;
import org.semanticwb.datamanager.DataList;
import org.semanticwb.datamanager.DataObject;
import org.semanticwb.datamanager.SWBDataSource;
import org.semanticwb.datamanager.script.ScriptObject;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Literal;
import org.semanticwb.store.Node;
import org.semanticwb.store.Resource;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleIterator;
import org.semanticwb.store.leveldb.GraphImp;

/**
 *
 * @author javiersolis
 */
public class SemDataStoreLevelDB implements SWBDataStore, SemDataStore {

    static private Logger log = Logger.getLogger(SemDataStoreLevelDB.class.getName());
    private static HashMap<String, Graph> graphs = new HashMap();
    ScriptObject dataStore = null;

    public SemDataStoreLevelDB(ScriptObject dataStore) {
        //System.out.println("DataStoreMongo:"+dataStore);
        this.dataStore = dataStore;
        try {
            initDB();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initDB() throws IOException {
        //graphs = new HashMap();
    }

    public Graph getGraph(String modelid) {
        Graph ret = graphs.get(modelid);
        if (ret == null) {
            synchronized (graphs) {
                ret = graphs.get(modelid);
                if (ret == null) {
                    HashMap<String, String> params = new HashMap();
                    params.put("path", dataStore.getString("path"));
                    try {
                        ret = new GraphImp(modelid, params);
                        ret.setTransactionEnabled(false);
                        graphs.put(modelid, ret);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return ret;
    }

    private Object toObject(Literal lit) {
        String type = lit.getType();
        String value = lit.getValue();

        if (type == null) {
            return value;
        } else if (type.endsWith("#boolean")) {
            return Boolean.parseBoolean(value);
        } else if (type.endsWith("#character")) {
            //ll=LiteralLabelFactory.create(tk4,tk3);
        } else if (type.endsWith("#decimal")) {
            return new BigDecimal(value);
        } else if (type.endsWith("#double")) {
            return Double.parseDouble(value);
        } else if (type.endsWith("#float")) {
            return Float.parseFloat(value);
        } else if (type.endsWith("#integer")) {
            return new BigInteger(value);
        } else if (type.endsWith("#int")) {
            return Integer.parseInt(value);
        } else if (type.endsWith("#long")) {
            return Long.parseLong(value);
        } else if (type.endsWith("#dateTime")) {
            //ll=LiteralLabelFactory.create( Long.parseLong(l.stringValue()));
        } else if (type.endsWith("#date")) {
            //ll=LiteralLabelFactory.create( Long.parseLong(l.stringValue()));
        }
        return value;
    }

    private boolean matchFilter(Triple triple, List<Triple> filter) {
        Iterator<Triple> it = filter.iterator();
        while (it.hasNext()) {
            Triple triple1 = it.next();

            Node s1 = triple.getSubject();
            Node p1 = triple.getProperty();
            Node o1 = triple.getObject();
            Node s2 = triple1.getSubject();
            Node p2 = triple1.getProperty();
            Node o2 = triple1.getObject();

            //System.out.println(triple+"->"+triple1);
            //System.out.println("s:"+(s2==null || s2.equals(s1))+" p:"+(p2==null || p2.equals(p1))+" o:"+(o2==null || o2.equals(o1)));
            if ((s2 == null || s2.equals(s1))
                    && (p2 == null || p2.equals(p1))
                    && (o2 == null || o2.equals(o1))) {
                return true;
            }
        }
        return false;
    }

    private DataList getData(TripleIterator it, List<Triple> filter) {
        DataList ndata = new DataList();
        addData(it, filter, ndata);
        return ndata;
    }
    
    //TODO:paginar    
    private void addData(TripleIterator it, List<Triple> filter, DataList ndata) {
        if (it == null) {
            return;
        }
        DataObject aux = null;
        String last_s = null;
        String last_p = null;
        boolean list_p =false;
        boolean isArray =false;
        int filterMatch = 0;
        while (it.hasNext()) {
            Triple triple = it.next();
            //System.out.println(triple);
            String s = triple.getSubject().getValue();
            String p = triple.getProperty().asResource().getUri();
            Node o = triple.getObject();
            
            if(p.equals(last_p))
            {                
                if(!list_p && !isArray)
                {
                    list_p=true;
                    isArray=true;
                }else
                {
                    list_p=false;
                }
            }else
            {
                list_p=false;
                isArray=false;
            }
           
            if (!s.equals(last_s)) {
                last_s = s;
                if (filter!=null && aux != null && filterMatch < filter.size()) {
                    ndata.remove(ndata.size() - 1);
                }
                aux = new DataObject();
                ndata.add(aux);
                filterMatch = 0;
                last_p=null;
            }

            if (filter!=null && matchFilter(triple, filter)) {
                filterMatch++;
                //System.out.println("filterMatch:"+filterMatch);
            }
            
            if (p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                aux.addParam("_id", "_suri:" + triple.getSubject().asResource().getUri());
            } else {
                //TODO:ns inverso
                int idx = p.lastIndexOf("#");
                if (idx == -1) {
                    idx = p.lastIndexOf("/");
                }
                
                String pname=p.substring(idx + 1);
                
                //cambiar propiedad a array
                if(list_p)
                {
                    Object v=aux.get(pname);
                    aux.addSubList(pname).add(v);
                }                
                
                if (o.isLiteral()) {
                    if(p.equals(last_p))
                    {
                        aux.getDataList(pname).add(toObject(o.asLiteral()));
                    }else
                    {
                        aux.addParam(pname, toObject(o.asLiteral()));
                    }
                } else if (o.isResource()) {
                    if (o.asResource().isBlank()) {
                        //TODO:recuperar objeto anidado
                    } else {
                        if(p.equals(last_p))
                        {
                            aux.getDataList(pname).add("_suri:" + o.asResource().getUri());
                        }else
                        {
                            aux.addParam(pname, "_suri:" + o.asResource().getUri());
                        }
                    }
                }
            }
            last_p=p;
        }
        if (filter!=null && aux != null && filterMatch < filter.size()) {
            ndata.remove(ndata.size() - 1);
        }
    }

    public DataObject fetch(DataObject json, SWBDataSource dataSource) throws IOException {
//        MongoClient mongoClient = new MongoClient("localhost");
        try {
            ScriptObject dss = dataSource.getDataSourceScript();
            String modelid = dss.getString("modelid");
            String scls = dss.getString("scls");

            Graph g = getGraph(modelid);

            long startRow = json.getLong("startRow", 0);
            long endRow = json.getLong("endRow", 0);
            String textMatchStyle = json.getString("textMatchStyle");
            DataObject data = json.getDataObject("data");
            DataObject oldValues = json.getDataObject("oldValues");
            DataList sortBy = json.getDataList("sortBy");

            DataObject ret = new DataObject();
            DataObject resp = new DataObject();

            ret.put("response", resp);
            resp.put("status", 0);
            resp.put("startRow", startRow);

            long endrow = startRow;

            TripleIterator it = null;
            List<Triple> filter = null;
            DataList ndata = new DataList();

            //System.out.println("request:" + json);
            if (data != null) {                
                Object oid = data.get("_id");                
                if (oid != null) {
                    if(oid instanceof DataList)
                    {
                        DataList<String> l=(DataList)oid;
                        for(String id: l)
                        {
                            it = g.findTriples(new Triple(toURI(id.toString()), null, null));
                            addData(it, null,ndata);                              
                        }
                    }else
                    {
                        it = g.findTriples(new Triple(toURI(oid.toString()), null, null));
                        addData(it, null,ndata);
                    }
                } else {
                    it = g.findTriples(new Triple((String) null, null, null), scls, null, null, false);
                    filter = toTriples(data, dataSource);
                    addData(it, filter,ndata);
                }
            } else {
                it = g.findTriples(new Triple((String) null, null, null), scls, null, null, false);
                filter = toTriples(data, dataSource);
                addData(it, filter,ndata);
            }
            
            resp.put("data", ndata);

            /*
             //textMatchStyle
             // exact
             // substring
             // startsWith
             if(data!=null && data.size()>0)
             {
             Iterator<String> it=data.keySet().iterator();
             while(it.hasNext())
             {
             String key=it.next();
             Object val=data.get(key);

             if(key.equals("_id"))
             {
             if(val instanceof BasicDBList)
             {
             data.put(key, new BasicDBObject().append("$in",val));
             }
             }else if(textMatchStyle!=null && val instanceof String)
             {
             if("substring".equals(textMatchStyle))
             {
             data.put(key, new BasicDBObject().append("$regex",val));
             }else if("startsWith".equals(textMatchStyle))
             {
             data.put(key, new BasicDBObject().append("$regex","^"+val));
             }
             }

             }
             }
             */
            resp.put("endRow", endrow);
            resp.put("totalRows", ndata.size());

            //System.out.println("response:" + ret);
            return ret;
        } finally {
//            mongoClient.close();
        }
    }

    public DataObject add(DataObject json, SWBDataSource dataSource) throws IOException {
        log.finest("Adding: " + json.toString());
        try {
            ScriptObject dss = dataSource.getDataSourceScript();
            String modelid = dss.getString("modelid");
            //String scls = dss.getString("scls");
            Graph g = getGraph(modelid);

            DataObject obj = json.getDataObject("data");

            if (obj.getString("_id") == null) {
                ObjectId id = new ObjectId();
                obj.put("_id", dataSource.getBaseUri() + id.toString());
                //obj.append("_id", id);
            }

            Iterator<Triple> it = toTriples(obj, dataSource).iterator();
            while (it.hasNext()) {
                Triple triple = it.next();
                if(triple.getObject()!=null)
                {
                    g.addTriple(triple);
                }
            }

            DataObject ret = new DataObject();
            DataObject resp = new DataObject();
            ret.put("response", resp);
            resp.put("status", 0);
            resp.put("data", obj);
            return ret;
        } finally {

        }
    }

    public DataObject remove(DataObject json, SWBDataSource dataSource) throws IOException {
        //        MongoClient mongoClient = new MongoClient("localhost");
        try {
            ScriptObject dss = dataSource.getDataSourceScript();
            String modelid = dss.getString("modelid");
            //String scls = dss.getString("scls");

            Graph g = getGraph(modelid);

            DataObject data = json.getDataObject("data");

            boolean removeByID = json.getBoolean("removeByID", true);

            if (removeByID) {
                String id = data.getString("_id");
                g.removeTriples(new Triple(toURI(id), null, null));
            } else {
                //TODO: borrado por query
            }

            DataObject ret = new DataObject();
            DataObject resp = new DataObject();
            ret.put("response", resp);
            resp.put("status", 0);

            return ret;
        } finally {
            //            mongoClient.close();
        }
    }

    public DataObject update(DataObject json, SWBDataSource dataSource) throws IOException 
    {
        try {
            ScriptObject dss = dataSource.getDataSourceScript();
            String modelid = dss.getString("modelid");
            String scls = dss.getString("scls");
            
            Graph g = getGraph(modelid);
            
            DataObject data = (DataObject) json.get("data");

            String id = data.getString("_id");
 
            List ltriples=toTriples(data, dataSource);
            //borrado
            //TODO: borrar solo una vez propiedades multiples
            Iterator<Triple> it = ltriples.iterator();
            while (it.hasNext()) {
                Triple triple = it.next();
                g.removeTriples(new Triple(triple.getSubject(),triple.getProperty(),null));
            }
            
            //alta
            it = ltriples.iterator();
            while (it.hasNext()) {
                Triple triple = it.next();
                if(triple.getObject()!=null)
                {
                    g.addTriple(triple);
                }
            }                   
            
            //recarga objeto completo
            TripleIterator it2 = g.findTriples(new Triple(toURI(id), null, null));
            DataList obj = getData(it2, new ArrayList());
            
            DataObject ret = new DataObject();
            DataObject resp = new DataObject();
            ret.put("response", resp);
            resp.put("status", 0);
            resp.put("data", obj.get(0));

            return ret;
        } finally {
            //            mongoClient.close();
        }
    }

    public void close() {
//        Iterator<Graph> it = graphs.values().iterator();
//        while (it.hasNext()) {
//            Graph graph = it.next();
//            graph.close();
//            it.remove();
//        }
    }

    private Node toNode(Object obj) {
        if (obj == null) {
            return null;
        }

        Node ret = null;

        if (obj instanceof String) {
            String str = (String) obj;
            Resource r = toURI(str);
            if (r != null) {
                ret = r;
            } else {
                ret = new Literal(str, null, null);
            }
        } else if (obj instanceof Integer) {
            ret = new Literal(obj.toString(), null, "<http://www.w3.org/2001/XMLSchema#int>");
        } else if (obj instanceof Long) {
            ret = new Literal(obj.toString(), null, "<http://www.w3.org/2001/XMLSchema#long>");
        } else if (obj instanceof Double) {
            ret = new Literal(obj.toString(), null, "<http://www.w3.org/2001/XMLSchema#double>");
        } else if (obj instanceof Float) {
            ret = new Literal(obj.toString(), null, "<http://www.w3.org/2001/XMLSchema#float>");
        } else if (obj instanceof Date) {
            ret = new Literal(obj.toString(), null, "<http://www.w3.org/2001/XMLSchema#date>");
        } else {
            ret = new Literal(obj.toString(), null, null);
        }
        return ret;
    }

    private void addTriples(Node subj, Node prop, Object obj, List<Triple> ret, ScriptObject dss) {
        //if(obj==null)return;
        if (obj instanceof DataObject) {
            Resource nobj = new Resource(new ObjectId().toString());
            ret.add(new Triple(subj, prop, nobj));

            String ns_prop = "http://swb.org/prop/" + dss.getString("dataStore") + "/" + dss.getString("modelid") + "/" + dss.getString("scls") + "#";

            Iterator<Map.Entry<String, Object>> it = ((DataObject) obj).entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Object> entry = it.next();
                String key = entry.getKey();
                Object obj2 = entry.getValue();
                Resource prop2 = new Resource("<" + ns_prop + key + ">");
                addTriples(nobj, prop2, obj2, ret, dss);
            }
        } else if (obj instanceof DataList) {
            DataList<Object> list = (DataList) obj;
            for (int x = 0; x < list.size(); x++) {
                addTriples(subj, prop, list.get(x), ret, dss);
            }
        } else {
            ret.add(new Triple(subj, prop, toNode(obj)));
        }
    }

    private List<Triple> toTriples(DataObject json, SWBDataSource ds) {
        if (json == null) {
            return new ArrayList();
        }
        ScriptObject dss=ds.getDataSourceScript();

        String ns_class = "http://swb.org/class/" + dss.getString("dataStore") + "/" + dss.getString("modelid") + "#";
        String ns_prop = "http://swb.org/prop/" + dss.getString("dataStore") + "/" + dss.getString("modelid") + "/" + dss.getString("scls") + "#";

        Resource subj = toURI(json.getId());
        Resource cls = new Resource("<" + ns_class + dss.getString("scls") + ">");
        Resource type = new Resource("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");

        ArrayList<Triple> ret = new ArrayList();
        Iterator<Map.Entry<String, Object>> it = json.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String key = entry.getKey();
            Object obj = entry.getValue();

            if (key.equals("_id")) {
                ret.add(new Triple(subj, type, cls));
            } else {
                Resource prop = new Resource("<" + ns_prop + key + ">");

                addTriples(subj, prop, obj, ret, dss);
            }
        }
        return ret;
    }

    private Resource toURI(String id) {
        if (id == null) {
            return null;
        }
        if (id.startsWith("_suri:")) {
            return new Resource("<" + id.substring(6) + ">");
        }
        return null;
    }

    @Override
    public DataObject aggregate(DataObject json, SWBDataSource dataSource) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
