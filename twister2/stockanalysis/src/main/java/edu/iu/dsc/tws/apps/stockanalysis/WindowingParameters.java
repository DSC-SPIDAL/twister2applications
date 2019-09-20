package edu.iu.dsc.tws.apps.stockanalysis;

import edu.iu.dsc.tws.task.window.constant.WindowType;

import java.io.Serializable;
import java.util.Objects;

public class WindowingParameters implements Serializable {

    private static final long serialVersionUID = 7215545889817090308L;

    private WindowType windowType;

    private long windowLength;

    private long slidingLength;

    private boolean isDuration;

    public WindowingParameters(WindowType windowType, long windowLength, long slidingLength, boolean isDuration) {
        this.windowType = windowType;
        this.windowLength = windowLength;
        this.slidingLength = slidingLength;
        this.isDuration = isDuration;
    }

    public WindowType getWindowType() {
        return windowType;
    }

    public void setWindowType(WindowType windowType) {
        this.windowType = windowType;
    }

    public long getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(long windowLength) {
        this.windowLength = windowLength;
    }

    public long getSlidingLength() {
        return slidingLength;
    }

    public void setSlidingLength(long slidingLength) {
        this.slidingLength = slidingLength;
    }

    public boolean isDuration() {
        return isDuration;
    }

    public void setDuration(boolean duration) {
        isDuration = duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowingParameters that = (WindowingParameters) o;
        return windowLength == that.windowLength
                && slidingLength == that.slidingLength
                && isDuration == that.isDuration
                && windowType == that.windowType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowType, windowLength, slidingLength, isDuration);
    }

    @Override
    public String toString() {
        return "WindowArguments{"
                + "windowType=" + windowType
                + ", windowLength=" + windowLength
                + ", slidingLength=" + slidingLength
                + ", isDuration=" + isDuration
                + '}';
    }
}



