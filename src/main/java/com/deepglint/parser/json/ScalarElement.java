package com.deepglint.parser.json;

import java.util.Iterator;
import java.util.LinkedList;

public class ScalarElement extends Element {
    private final String fScalar;

    protected ScalarElement(int aType, String aValue) {
        super(aType);

        assert aValue != null;

        assert aType >= 1 && aType <= 4;

        this.fScalar = aValue;
    }

    public ScalarElement(String aValue) {
        super(4);

        assert aValue != null;

        this.fScalar = aValue;
    }

    public ScalarElement(boolean aValue) {
        super(2);
        this.fScalar = String.valueOf(aValue);
    }

    public ScalarElement(long aValue) {
        super(3);
        this.fScalar = String.valueOf(aValue);
    }

    public ScalarElement(double aValue) {
        super(3);
        this.fScalar = String.valueOf(aValue);
    }

    public ScalarElement() {
        super(1);
        this.fScalar = "null";
    }

    public boolean isScalar() {
        assert this.fScalar != null;

        return true;
    }

    public boolean isObject() {
        assert this.fScalar != null;

        return false;
    }

    public boolean isArray() {
        assert this.fScalar != null;

        return false;
    }

    public String toString() {
        assert this.fScalar != null;

        return this.fType == 4 ? '"' + this.fScalar + '"' : this.fScalar;
    }

    public int size() {
        assert this.fScalar != null;

        return 0;
    }

    public boolean hasKey(String aKey) {
        assert aKey != null;

        assert this.fScalar != null;

        return false;
    }

    public boolean hasIndex(Integer aIndex) {
        assert aIndex >= 0;

        assert this.fScalar != null;

        return false;
    }

    public Element getChildElement(String aKey) {
        assert aKey != null;

        assert this.fScalar != null;

        return null;
    }

    public Element getChildElement(Integer aIndex) {
        assert aIndex >= 0;

        assert this.fScalar != null;

        return null;
    }

    public Iterator<Element> getChildElements() {
        return null;
    }

    public LinkedList<Element> getDescendentElements(String aKey, LinkedList<Element> aList) {
        assert aKey != null;

        assert aList != null;

        return aList;
    }

    public LinkedList<Element> getScalarElements(LinkedList<Element> aList) {
        assert aList != null;

        aList.add(this);
        return aList;
    }

    @Override
    public Element getChildElement(int depth, Path aPath) {
        return null;
    }
}
