package com.deepglint.parser.json;

import java.util.Iterator;
import java.util.LinkedList;

public abstract class Element {
    public static final int NULL = 1;
    public static final int BOOLEAN = 2;
    public static final int NUMBER = 3;
    public static final int STRING = 4;
    public static final int OBJECT = 5;
    public static final int ARRAY = 6;
    protected final int fType;
    protected Element iParent;
    protected String iKey;
    protected int iIndex;

    protected Element(int aType) {
        this.fType = aType;
    }

    public int type() {
        assert this.fType > 0;

        return this.fType;
    }

    public Element getParentElement() {
        return this.iParent;
    }

    public String getKey() {
        return this.iKey;
    }

    public int getIndex() {
        return this.iIndex;
    }

    public abstract String toString();

    public abstract int size();

    public abstract boolean isScalar();

    public abstract boolean isObject();

    public abstract boolean isArray();

    public abstract boolean hasKey(String var1);

    public abstract boolean hasIndex(Integer var1);

    public abstract Element getChildElement(String var1);

    public abstract Element getChildElement(Integer var1);

    public abstract Iterator<Element> getChildElements();

    public abstract LinkedList<Element> getDescendentElements(String var1, LinkedList<Element> var2);

    public abstract LinkedList<Element> getScalarElements(LinkedList<Element> var1);

    public abstract Element getChildElement(int depth, Path aPath);
}
