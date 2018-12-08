package com.tools.data.db.metadata;

public interface Element {

    /**
     * Returns the metadata element which is the parent of this metadata
     * element.
     *
     * @return the parent.
     */
    public Element getParent();

    /**
     * Returns the name of this metadata element or {@code null} if
     * this element has no name.
     *
     * @return the name.
     */
    public String getName();

}
