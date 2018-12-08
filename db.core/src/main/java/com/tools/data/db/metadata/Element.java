package com.tools.data.db.metadata;

public abstract class Element {

    /**
     * Returns the metadata element which is the parent of this metadata
     * element.
     *
     * @return the parent.
     */
    public abstract Element getParent();

    /**
     * Returns the name of this metadata element or {@code null} if
     * this element has no name.
     *
     * @return the name.
     */
    public abstract String getName();

    public <T> T to(Class<? extends T> clazz){
        return (T)this;
    }

}
