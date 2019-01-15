package edu.caltech.nanodb.server.properties;


import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This is the central location where all properties exposed by the database
 * can be registered and accessed.  Various components register their
 * configurable properties with the registry, and then they can be accessed
 * and/or set from the SQL prompt.
 */
public class PropertyRegistry {

    private class PropertyDescriptor {
        /** The name of the property. */
        String name;

        /** The current value of the property. */
        Object value;

        /**
         * A flag indicating whether the property is read-only or read-write.
         */
        boolean readonly;

        /** A validator to ensure the property's values are correct. */
        PropertyValidator validator;


        PropertyDescriptor(String name, PropertyValidator validator,
                           Object initialValue, boolean readonly) {
            this.name = name;
            this.validator = validator;

            // Set readonly to false for initial write.
            this.readonly = false;
            setValue(initialValue);

            // Now, set readonly flag to what it should be.
            this.readonly = readonly;
        }


        public Object getValue() {
            return value;
        }


        public void setValue(Object newValue) {
            if (!setup && readonly) {
                throw new PropertyException("Property \"" + name +
                    "\" is read-only during normal operation, and should " +
                    "only be set at start-up.");
            }

            value = validator.validate(newValue);
        }
    }


    /**
     * A mapping of property names to values.  A thread-safe hash map is used
     * since this will be accessed and mutated from different threads.
     */
    private ConcurrentHashMap<String, PropertyDescriptor> properties =
        new ConcurrentHashMap<>();


    /**
     * This flag indicates whether the database is still in "start-up mode"
     * or not.  During start-up mode, all read-only properties may also be
     * modified.
     */
    private boolean setup = true;


    /**
     * Records that setup has been completed, and read-only properties
     * should no longer be allowed to change.
     */
    public void setupCompleted() {
        setup = false;
    }


    /**
     * Add a read-only or read-write property to the registry, along with a
     * type and an initial value.
     *
     * @param name the name of the property
     * @param validator a validator for the property
     * @param initialValue an initial value for the property
     * @param readonly a flag indicating whether the property is read-only
     *        ({@code true}) or read-write ({@code false})
     */
    public void addProperty(String name, PropertyValidator validator,
                            Object initialValue, boolean readonly) {
        properties.put(name,
            new PropertyDescriptor(name, validator, initialValue, readonly));
    }


    /**
     * Add a read-write property to the registry, along with a
     * type and an initial value.
     *
     * @param name the name of the property
     * @param validator a validator for the property
     * @param initialValue an initial value for the property
     */
    public void addProperty(String name, PropertyValidator validator,
                            Object initialValue) {
        addProperty(name, validator, initialValue, false);
    }


    /**
     * Returns {@code true} if the server has a property of the specified
     * name, {@code false} otherwise.
     *
     * @param name the non-null name of the property
     * @return {@code true} if the server has a property of the specified
     *         name, {@code false} otherwise.
     */
    public boolean hasProperty(String name) {
        if (name == null)
            throw new IllegalArgumentException("name cannot be null");

        return properties.containsKey(name);
    }


    /**
     * Returns an unmodifiable set of all property names.
     *
     * @return an unmodifiable set of all property names.
     */
    public Set<String> getAllPropertyNames() {
        return Collections.unmodifiableSet(properties.keySet());
    }


    public Object getPropertyValue(String name)
        throws PropertyException {

        if (name == null)
            throw new IllegalArgumentException("name cannot be null");

        if (!properties.containsKey(name)) {
            throw new PropertyException("No property named \"" +
                name + "\"");
        }

        return properties.get(name).getValue();
    }


    public void setPropertyValue(String name, Object value) {
        if (name == null)
            throw new IllegalArgumentException("name cannot be null");

        if (!properties.containsKey(name)) {
            throw new PropertyException("No property named \"" +
                name + "\"");
        }

        properties.get(name).setValue(value);
    }


    /**
     * Returns a property's value as a Boolean.  If the property's value is
     * not a Boolean then an exception is reported.
     *
     * @param name the name of the property to fetch
     *
     * @return a Boolean true or false value for the property
     */
    public boolean getBooleanProperty(String name) {
        return (Boolean) getPropertyValue(name);
    }


    /**
     * Returns a property's value as a String.  If the property's value is
     * not a String then an exception is reported.
     *
     * @param name the name of the property to fetch
     *
     * @return a String value for the property
     */
    public String getStringProperty(String name) {
        return (String) getPropertyValue(name);
    }


    /**
     * Returns a property's value as an integer.  If the property's value is
     * not an integer then an exception is reported.
     *
     * @param name the name of the property to fetch
     *
     * @return an integer value for the property
     */
    public int getIntProperty(String name) {
        return (Integer) getPropertyValue(name);
    }
}
