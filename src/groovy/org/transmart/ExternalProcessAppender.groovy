package org.transmart

import com.google.common.base.Charsets
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent
import org.transmartproject.core.exceptions.InvalidArgumentsException

import static java.lang.ProcessBuilder.Redirect.*

@Log4j
@CompileStatic
class ExternalProcessAppender extends AppenderSkeleton {

    List<String> command
    int restartWindow
    int restartLimit
    boolean throwOnFailure = false

    protected long starttime
    protected int failcount = 0
    protected Process process
    protected Writer input
    protected boolean failed = false

    private ThreadLocal<Integer> recursionCount = new ThreadLocal<Integer>()

    void setRestartLimit(int l) {
        if (l < 0) throw new InvalidArgumentsException("restartLimit cannot be negative (use 0 to disable the limit)")
        this.restartLimit = l
    }

    void setRestartWindow(int w) {
        if (w <= 0) throw new InvalidArgumentsException("restartWindow must be larger than 0")
        this.restartWindow = w
    }

    ExternalProcessAppender() {
        recursionCount.set(0)
    }

//    ExternalProcessAppender(Map<String, Object> opts) {
//        name = (String) opts.name
//        assert name, "No valid name : String provided"
//        command = (List<String>) opts.command
//        assert command, "No valid command : List<String> provided"
//        restartLimit = (int) opts.get("restartLimit", 10)
//        assert restartLimit >= 0, "No valid restartLimit : int >= 0 provided (0 for indefinite restarts)"
//        restartWindow = (int) opts.get("restartWindow", 10)
//        if (restartLimit) {
//            assert restartWindow > 0, "No valid restartWindow : int > 0 provided"
//        }
//    }

    private synchronized startProcess() {
        if (process == null) {
            starttime = System.currentTimeMillis()
        }
        process = new ProcessBuilder(command).redirectOutput(INHERIT).redirectError(INHERIT).start()
        input = new OutputStreamWriter(process.getOutputStream(), Charsets.UTF_8)
    }

    synchronized boolean getChildAlive() {
        if (process == null) return false
        try {
            process.exitValue()
            return false
        } catch (IllegalThreadStateException _) {
            return true
        }
    }

    private synchronized write(String str) {
        boolean retry = true
        while (retry) {
            try {
                input.write(str)
                input.flush()
                return
            } catch (IOException e) {
                if (childAlive) {
                    throw e
                }
                retry = restartChild()
            }
        }
    }

    private synchronized boolean restartChild() {
        failcount++
        int restartWindowEnd = starttime + restartWindow * 1000
        long now = System.currentTimeMillis()
        boolean inWindow = restartLimit == 0 ? false : now > restartWindowEnd
        if (restartLimit != 0 && inWindow && failcount > restartLimit) {
            failed = true
            return false
        }
        input.close()
        if (!inWindow) {
            starttime = now
        }
        startProcess()
        return true
    }

    static breakpoint() {
        //log.info("In test breakpoint")
        //sleep(10000)
    }

    void append(LoggingEvent event) {

        System.currentTimeMillis()
    }

    @Override
    boolean requiresLayout() {return false}

    @Override
    synchronized void close() {
        input.close()
    }
}
