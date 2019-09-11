package tw;

import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Node implements WritableComparable<Node> {

    private static int kVals[]={1,2};
    private int nodeId;
    private List<Integer> adjList;
    private boolean isObj;
    private boolean isActive;
    private Map<Integer, Integer> kMap;
    private static Gson gson = new Gson();

    public Node(){

    }

    public Node(int nodeId) {
        this.nodeId = nodeId;
        this.isObj = false;
        this.isActive = false;
        this.kMap = new HashMap<>();
        this.adjList=new ArrayList<>();
    }

    public Node(int nodeId, List<Integer> adjList, boolean isObj, boolean isActive, Map<Integer, Integer> kMap) {
        this.nodeId = nodeId;
        this.adjList = adjList;
        this.isObj = isObj;
        this.isActive = isActive;
        this.kMap = kMap;
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<Integer> getAdjList() {
        return adjList;
    }

    public boolean getIsObj() {
        return isObj;
    }

    public boolean isActive() {
        return isActive;
    }

    public Map<Integer, Integer> getkMap() {
        return kMap;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setAdjList(List<Integer> adjList) {
        this.adjList = adjList;
    }

    public void setIsObj(boolean isObj) {
        this.isObj = isObj;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public void setkMap(Map<Integer, Integer> kMap) {
        this.kMap = kMap;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, gson.toJson(this));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Node node = gson.fromJson(WritableUtils.readString(in) , Node.class);
        this.nodeId = node.nodeId;
        this.adjList = node.adjList;
        this.isActive = node.isActive;
        this.isObj = node.isObj;
        this.kMap = node.kMap;
    }

//    @Override
//    public int hashCode()
//    {
//        return node*163 + dir;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (o instanceof Node) {
//            Node ip = (Node) o;
//            return dir == ip.dir && node == ip.node;
//        }
//        return false;
//    }

    @Override
    public int compareTo(Node ip) {
        int cmp = Integer.compare(this.nodeId, ip.nodeId);
        return cmp;
    }
    /**
     * Convenience method for comparing two ints.
     */
//    public static int compare(int a, int b) {
//        return (a < b ? -1 : (a == b ? 0 : 1));
//    }
//
//    public static int compareChar(char a, char b) {
//        return (a < b ? -1 : (a == b ? 0 : 1));
//    }


    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public static Node jsonToNode(String s){
        return gson.fromJson(s, Node.class);
    }
}

