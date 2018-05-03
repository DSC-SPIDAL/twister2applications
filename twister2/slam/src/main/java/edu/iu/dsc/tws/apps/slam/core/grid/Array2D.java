package edu.iu.dsc.tws.apps.slam.core.grid;

import edu.iu.dsc.tws.apps.slam.core.utils.IntPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Array2D <T> {
    private static Logger LOG = LoggerFactory.getLogger(Array2D.class);

    public int xsize, ysize;
    public Object cells[][];

    public Array2D() {
    }

    public Array2D(int xsize, int ysize) {
        this.xsize = xsize;
        this.ysize = ysize;
        if (this.xsize > 0 && this.ysize > 0) {
            cells = new Object[this.xsize][this.ysize];

        } else {
            this.xsize = this.ysize = 0;
            cells = null;
        }
        LOG.debug("xsize= " + this.xsize + " ysize: " + this.ysize);
    }

    public void assign(Array2D g) {
        if (xsize != g.xsize || ysize != g.ysize) {
            xsize = g.xsize;
            ysize = g.ysize;
            if (xsize > 0 && ysize > 0) {
                cells = new Object[xsize][ysize];
            } else {
                xsize = ysize = 0;
                cells = null;
            }
        }
        for (int x = 0; x < xsize; x++) {
            System.arraycopy(g.cells[x], 0, cells[x], 0, ysize);
        }

        LOG.debug("xsize= " + xsize + " ysize: " + ysize);
    }

    public Array2D(Array2D g) {
        assign(g);
    }

    public int getPatchSize() {
        return 0;
    }

    public int getPatchMagnitude() {
        return 0;
    }

    public int getXSize() {
        return xsize;
    }

    public int getYSize() {
        return ysize;
    }

    public void clear() {
        LOG.debug("xsize= " + xsize + " ysize: " + ysize);
        cells = null;
        xsize = 0;
        ysize = 0;
    }

    public void resize(int xmin, int ymin, int xmax, int ymax) {
        int xsize = xmax - xmin;
        int ysize = ymax - ymin;
        Object[][] newcells = new Object[xsize][ysize];
        int dx = xmin < 0 ? 0 : xmin;
        int dy = ymin < 0 ? 0 : ymin;
        int Dx = xmax < this.xsize ? xmax : this.xsize;
        int Dy = ymax < this.ysize ? ymax : this.ysize;
        for (int x = dx; x < Dx; x++) {
            System.arraycopy(this.cells[x], dy, newcells[x - xmin], dy - ymin, Dy - dy);
        }
        this.cells = newcells;
        this.xsize = xsize;
        this.ysize = ysize;
    }

    public boolean isInside(int x, int y) {
        return x >= 0 && y >= 0 && x < xsize && y < ysize;
    }

    public boolean isInside(IntPoint p) {
        return p.x >= 0 && p.y >= 0 && p.x < xsize && p.y < ysize;
    }

    public Object cell(IntPoint p) {
        assert (isInside(p.x, p.y));
        return cells[p.x][p.y];
    }

    public Object cell(int x, int y) {
        assert (isInside(x, y));
        return cells[x][y];
    }

    public int getXsize() {
        return xsize;
    }

    public int getYsize() {
        return ysize;
    }

    public Object[][] getCells() {
        return cells;
    }

    public void setXsize(int xsize) {
        this.xsize = xsize;
    }

    public void setYsize(int ysize) {
        this.ysize = ysize;
    }

    public void setCells(Object[][] cells) {
        this.cells = cells;
    }
}
