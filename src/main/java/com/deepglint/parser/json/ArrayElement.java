package com.deepglint.parser.json;

import java.util.Iterator;
import java.util.LinkedList;

public class ArrayElement extends Element {
    private final LinkedList<Element> fArray;

    public ArrayElement() {
        super(6);
        this.fArray = new LinkedList();
    }

    public ArrayElement(LinkedList<Element> aArray) {
        super(6);

        assert aArray != null;

        this.fArray = aArray;
    }

    public void addElement(Element aElement) {
        assert aElement != null;

        assert this.fArray != null;

        this.fArray.add(aElement);
        aElement.iParent = this;
        aElement.iIndex = this.fArray.size() - 1;
        aElement.iKey = null;
    }

    public boolean isScalar() {
        assert this.fArray != null;

        return false;
    }

    public boolean isObject() {
        assert this.fArray != null;

        return false;
    }

    public boolean isArray() {
        assert this.fArray != null;

        return true;
    }

    public String toString() {
        assert this.fArray != null;

        if (this.fArray.size() == 0) {
            return "[]";
        } else {
            StringBuilder buffer = new StringBuilder();
            Iterator iterator = this.fArray.iterator();

            while(iterator.hasNext()) {
                buffer.append(',');
                buffer.append(iterator.next());
            }

            return "[" + buffer.substring(1) + "]";
        }
    }

    public int size() {
        assert this.fArray != null;

        return this.fArray.size();
    }

    public boolean hasKey(String aKey) {
        assert aKey != null;

        assert this.fArray != null;

        return false;
    }

    public boolean hasIndex(Integer aIndex) {
        assert aIndex >= 0;

        assert this.fArray != null;

        return aIndex < this.fArray.size();
    }

    public Element getChildElement(String aKey) {
        assert aKey != null;

        assert this.fArray != null;

        return null;
    }

    public Element getChildElement(Integer aIndex) {
        assert aIndex >= 0;

        assert this.fArray != null;

        return aIndex >= this.fArray.size() ? null : (Element)this.fArray.get(aIndex);
    }

    public Iterator<Element> getChildElements() {
        assert this.fArray != null;

        return this.fArray.iterator();
    }

    public LinkedList<Element> getDescendentElements(String aKey, LinkedList<Element> aList) {
        assert aKey != null;

        assert aList != null;

        Iterator iterator = this.getChildElements();

        while(iterator.hasNext()) {
            ((Element)iterator.next()).getDescendentElements(aKey, aList);
        }

        return aList;
    }

    public LinkedList<Element> getScalarElements(LinkedList<Element> aList) {
        assert aList != null;

        Iterator iterator = this.getChildElements();

        while(iterator.hasNext()) {
            ((Element)iterator.next()).getScalarElements(aList);
        }

        return aList;
    }

    @Override
    public Element getChildElement(int depth, Path aPath) {
        StringBuilder sb = new StringBuilder();
        for(Element element : fArray){
            for (int i = depth; i < aPath.fDepth; ++i) {
                if (aPath.fKeys[i] != null) {
                    element = element.getChildElement(aPath.fKeys[i]);
                }else{
                    element = null;
                }
            }
            if(element != null){
                sb.append(element.toString());
                sb.append(',');
            }
        }
        if(sb.length() > 0){
            sb.delete(sb.length()-1, sb.length());
        }
        Element element = new ScalarElement(sb.toString());
        return element;
    }
}
