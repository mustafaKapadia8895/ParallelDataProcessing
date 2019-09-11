package tw;

import java.io.*; import org.apache.hadoop.io.*;

public class StringInt implements WritableComparable<StringInt> {
    private char dir; private int node;
    public StringInt() {}

    public StringInt(char first, int second)
    {
        set(first, second);
    }

    public void set(char first, int second)
    {
        this.dir = first;
        this.node = second;
    }

    public char getDir()
    {
        return dir;
    }

    public int getNode()
    {
        return node;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChar(dir);
        out.writeInt(node);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dir = in.readChar();
        node = in.readInt();
    }

    @Override
    public int hashCode()
    {
        return node*163 + dir;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StringInt) {
            StringInt ip = (StringInt) o;
            return dir == ip.dir && node == ip.node;
        }
        return false;
    }

    @Override
    public int compareTo(StringInt ip) {
        int cmp = Character.compare(dir, ip.dir);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(node, ip.node);
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
}

