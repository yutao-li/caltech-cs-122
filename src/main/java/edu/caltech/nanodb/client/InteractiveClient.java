package edu.caltech.nanodb.client;


import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.caltech.nanodb.server.CommandResult;


/**
 * This abstract class implements the basic functionality necessary for
 * providing an interactive SQL client.
 */
public abstract class InteractiveClient {

    private static Logger logger = LogManager.getLogger(InteractiveClient.class);


    /** A string constant specifying the "first-line" command-prompt. */
    private static final String CMDPROMPT_FIRST = "CMD> ";


    /** A string constant specifying the "subsequent-lines" command-prompt. */
    private static final String CMDPROMPT_NEXT = "   > ";

    /** The buffer that accumulates each command's text. */
    private StringBuilder enteredText;


    public abstract void startup() throws Exception;


    public void mainloop() {
        // We don't use the console directly, since we can't read/write it
        // if someone redirects a file onto the client's input-stream.
        boolean hasConsole = (System.console() != null);

        if (hasConsole) {
            System.out.println(
                "Welcome to NanoDB.  Exit with EXIT or QUIT command.\n");
        }

        boolean exiting = false;
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(System.in));
        while (!exiting) {
            enteredText = new StringBuilder();
            boolean firstLine = true;

getcmd:
            while (true) {
                try {
                    if (hasConsole) {
                        if (firstLine) {
                            System.out.print(CMDPROMPT_FIRST);
                            System.out.flush();
                            firstLine = false;
                        }
                        else {
                            System.out.print(CMDPROMPT_NEXT);
                            System.out.flush();
                        }
                    }

                    String line = bufReader.readLine();
                    if (line == null) {
                        // Hit EOF.
                        exiting = true;
                        break;
                    }

                    enteredText.append(line).append('\n');

                    // Process any commands in the entered text.
                    while (true) {
                        String command = getCommandString();
                        if (command == null)
                            break;  // No more complete commands.

                        // if (logger.isDebugEnabled())
                        //     logger.debug("Command string:\n" + command);

                        CommandResult result = handleCommand(command);
                        if (result.isExit()) {
                            exiting = true;
                            break getcmd;
                        }
                    }

                    if (enteredText.length() == 0)
                        firstLine = true;
                }
                catch (Throwable e) {
                    System.out.println("Unexpected error:  " + e.getClass() +
                        ":  " + e.getMessage());
                    logger.error("Unexpected error", e);
                }
            }
        }
    }


    /**
     * This helper method goes through the {@link #enteredText} buffer, trying
     * to identify the extent of the next command string.  This is done using
     * semicolons (that are not enclosed with single or double quotes).  If a
     * command is identified, it is removed from the internal buffer and
     * returned.  If no complete command is identified, {@code null} is
     * returned.
     *
     * @return the first semicolon-terminated command in the internal data
     *         buffer, or {@code null} if the buffer contains no complete
     *         commands.
     */
    private String getCommandString() {
        int i = 0;
        String command = null;

        while (i < enteredText.length()) {
            char ch = enteredText.charAt(i);
            if (ch == ';') {
                // Found the end of the command.  Extract the string, and
                // make sure the semicolon is also included.
                command = enteredText.substring(0, i + 1);
                enteredText.delete(0, i + 1);

                // Consume any leading whitespace at the start of the entered
                // text.
                while (enteredText.length() > 0 &&
                       Character.isWhitespace(enteredText.charAt(0))) {
                    enteredText.deleteCharAt(0);
                }

                break;
            }
            else if (ch == '\'' || ch == '"') {
                // Need to ignore all subsequent characters until we find
                // the end of this quoted string.
                i++;
                while (i < enteredText.length() &&
                       enteredText.charAt(i) != ch) {
                    i++;
                }
            }

            i++;  // Go on to the next character.
        }

        return command;
    }


    /**
     * Subclasses can implement this method to handle each command entered
     * by the user.  For example, a subclass may send the command over a
     * socket to the server, wait for a response, then output the response
     * to the console.
     *
     * @param command the command to handle.
     *
     * @return the command-result from executing the command
     */
    public abstract CommandResult handleCommand(String command);


    /**
     * Shut down the interactive client.  The specific way the client
     * interacts with the server dictates how this shutdown mechanism
     * will work.
     *
     * @throws Exception if any error occurs during shutdown
     */
    public abstract void shutdown() throws Exception;
}
