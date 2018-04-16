package com.deepglint.parser.json;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Map.Entry;

public class ObjectElement extends Element {
    private final TreeMap<String, Element> fObject;

    public ObjectElement() {
        super(5);
        this.fObject = new TreeMap();
    }

    public ObjectElement(TreeMap<String, Element> aObject) {
        super(5);

        assert aObject != null;

        this.fObject = aObject;
    }

    public void putElement(String aKey, Element aElement) {
        assert aKey != null;

        assert aElement != null;

        assert this.fObject != null;

        this.fObject.put(aKey, aElement);
        aElement.iParent = this;
        aElement.iKey = aKey;
        aElement.iIndex = -1;
    }

    public boolean isScalar() {
        assert this.fObject != null;

        return false;
    }

    public boolean isObject() {
        assert this.fObject != null;

        return true;
    }

    public boolean isArray() {
        assert this.fObject != null;

        return false;
    }

    public String toString() {
        assert this.fObject != null;

        if (this.fObject.size() == 0) {
            return "{}";
        } else {
            StringBuilder buffer = new StringBuilder();
            Iterator iterator = this.fObject.entrySet().iterator();

            while(iterator.hasNext()) {
                Entry<String, Element> entry = (Entry)iterator.next();
                String key = (String)entry.getKey();
                Element element = (Element)entry.getValue();
                buffer.append(',');
                buffer.append('"');
                buffer.append(key.toString());
                buffer.append('"');
                buffer.append(':');
                buffer.append(element.toString());
            }

            return "{" + buffer.substring(1) + "}";
        }
    }

    public int size() {
        assert this.fObject != null;

        return this.fObject.size();
    }

    public boolean hasKey(String aKey) {
        assert aKey != null;

        assert this.fObject != null;

        return this.fObject.containsKey(aKey);
    }

    public boolean hasIndex(Integer aIndex) {
        assert this.fObject != null;

        return false;
    }

    public Element getChildElement(String aKey) {
        assert this.fObject != null;

        return (Element)this.fObject.get(aKey);
    }

    public Element getChildElement(Integer aIndex) {
        assert aIndex >= 0;

        assert this.fObject != null;

        return null;
    }

    public Iterator<Element> getChildElements() {
        assert this.fObject != null;

        return this.fObject.values().iterator();
    }

    public LinkedList<Element> getDescendentElements(String aKey, LinkedList<Element> aList) {
        assert aKey != null;

        assert aList != null;

        Iterator iterator = this.getChildElements();

        while(iterator.hasNext()) {
            Element element = (Element)iterator.next();
            if (element.iKey.equals(aKey)) {
                aList.add(element);
            } else {
                element.getDescendentElements(aKey, aList);
            }
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
        return null;
    }

    public Iterator<String> getKeyIterator() {
        assert this.fObject != null;

        return this.fObject.keySet().iterator();
    }
}
