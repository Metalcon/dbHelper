package de.metalcon.dbhelper;

import java.lang.invoke.MethodHandles;

import org.junit.Test;

public class OptionsTest extends Options {

    static {
        try {
            Options.initialize("testOptions.txt", MethodHandles.lookup()
                    .lookupClass());
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    static String s;

    @Test
    public void checkVariables() {

    }
}
