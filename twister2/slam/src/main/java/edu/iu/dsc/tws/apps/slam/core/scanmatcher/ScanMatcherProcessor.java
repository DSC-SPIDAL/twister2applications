package edu.iu.dsc.tws.apps.slam.core.scanmatcher;

import edu.iu.dsc.tws.apps.slam.core.grid.GMap;
import edu.iu.dsc.tws.apps.slam.core.grid.IGMap;
import edu.iu.dsc.tws.apps.slam.core.grid.MapFactory;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeReading;
import edu.iu.dsc.tws.apps.slam.core.sensor.RangeSensor;
import edu.iu.dsc.tws.apps.slam.core.sensor.Sensor;
import edu.iu.dsc.tws.apps.slam.core.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ScanMatcherProcessor {
    private static Logger LOG = LoggerFactory.getLogger(ScanMatcherProcessor.class);

    ScanMatcher m_matcher;
    boolean m_computeCovariance;
    boolean m_first;
    Map<String, Sensor> m_sensorMap;
    double m_regScore, m_critScore;
    int m_beams;
    double m_maxMove;
    //state
    IGMap m_map;
    DoubleOrientedPoint m_pose;
    DoubleOrientedPoint m_odoPose;
    int  m_count;

    boolean useICP;

    public ScanMatcherProcessor(IGMap m) {
        m_map = MapFactory.create(m.getCenter(), m.getWorldSizeX(), m.getWorldSizeY(), m.getResolution());
        m_pose = new DoubleOrientedPoint(0.0,0.0,0.0);
        m_regScore=300;
        m_critScore=.5*m_regScore;
        m_maxMove=1;
        m_beams=0;
        m_computeCovariance=false;
        //m_eigenspace=gsl_eigen_symmv_alloc(3);
        useICP=false;
    }

    public ScanMatcherProcessor
            (double xmin, double ymin, double xmax, double ymax, double delta, double patchdelta) {
        m_map = MapFactory.create(new DoublePoint((xmax + xmin) * .5, (ymax + ymin) * .5), xmax - xmin, ymax - ymin, patchdelta);
        m_pose = new DoubleOrientedPoint(0.0,0.0,0.0);
        m_regScore=300;
        m_critScore=.5*m_regScore;
        m_maxMove=1;
        m_beams=0;
        m_computeCovariance=false;
        //m_eigenspace=gsl_eigen_symmv_alloc(3);
        useICP=false;
    }



    public void init(){
        m_first=true;
        m_pose= new DoubleOrientedPoint(0.0,0.0,0.0);
        m_count=0;
    }

    void processScan(RangeReading reading){
        /**retireve the position from the reading, and compute the odometry*/
        DoubleOrientedPoint relPose = reading.getPose();
        if (m_count == 0) {
            m_odoPose = relPose;
        }

        //compute the move in the scan matcher
        //reference frame

        DoubleOrientedPoint move = DoubleOrientedPoint.minus(relPose, m_odoPose);
        double dth=m_odoPose.theta-m_pose.theta;
        // cout << "rel-move x="<< move.x <<  " y=" << move.y << " theta=" << move.theta << endl;

        double lin_move= DoubleOrientedPoint.mulD(move, move);
        if (lin_move>m_maxMove){
            LOG.error("Too big jump in the log file: {} relPose= {} {} ignoring",  lin_move, relPose.x, relPose.y);
            return;
            //assert(0);
            //dth=0;
            //move.x= move.y=move.theta=0.0;
        }

        double s=Math.sin(dth), c=Math.cos(dth);
        DoubleOrientedPoint dPose = new DoubleOrientedPoint(0.0, 0.0, 0.0);
        dPose.x=c*move.x-s*move.y;
        dPose.y=s*move.x+c*move.y;
        dPose.theta=move.theta;


        LOG.debug("abs-move x=" + dPose.x +  " y=" + dPose.y + " theta=" + dPose.theta);

        m_pose = DoubleOrientedPoint.plus(m_pose, dPose);
        m_pose.theta=Math.atan2(Math.sin(m_pose.theta), Math.cos(m_pose.theta));

        LOG.debug(m_pose.x + " y=" + m_pose.y + " theta=" + m_pose.theta);
        
        m_odoPose=relPose; //update the past pose for the next iteration


        //FIXME here I assume that everithing is referred to the center of the robot,
        //while the offset of the laser has to be taken into account

        assert(reading.size()==m_beams);
        /*
            double * plainReading = new double[beams];
        #ifdef SCANMATHCERPROCESSOR_DEBUG
            cout << "PackedReadings ";
        #endif
            for(unsigned int i=0; i<beams; i++){
                plainReading[i]=reading[i];
        #ifdef SCANMATHCERPROCESSOR_DEBUG
                cout << plainReading[i] << " ";
        #endif
            }
        */
        double []plainReading = new double[m_beams];
//        reading.rawView(plainReading, m_map.getDelta());

        //the final stuff: scan match the pose
        double score=0;
        DoubleOrientedPoint newPose=m_pose;
        if (m_count > 0){
            if(m_computeCovariance){
                Covariance3 cov = new Covariance3();
                score = m_matcher.optimize(newPose, cov, m_map, m_pose, plainReading);
                        /*
			gsl_matrix* m=gsl_matrix_alloc(3,3);
			gsl_matrix_set(m,0,0,cov.xx); gsl_matrix_set(m,0,1,cov.xy); gsl_matrix_set(m,0,2,cov.xt);
			gsl_matrix_set(m,1,0,cov.xy); gsl_matrix_set(m,1,1,cov.yy); gsl_matrix_set(m,1,2,cov.yt);
			gsl_matrix_set(m,2,0,cov.xt); gsl_matrix_set(m,2,1,cov.yt); gsl_matrix_set(m,2,2,cov.tt);
			gsl_matrix* evec=gsl_matrix_alloc(3,3);
			gsl_vector* eval=gsl_vector_alloc(3);
                        */
                double m[][] = new double[3][3];
                double evec[][] = new double[3][3];
                double eval[] = new double[3];
                m[0][0] = cov.xx;
                m[0][1] = cov.xy;
                m[0][2] = cov.xt;
                m[1][0] = cov.xy;
                m[1][1] = cov.yy;
                m[1][2] = cov.yt;
                m[2][0] = cov.xt;
                m[2][1] = cov.yt;
                m[2][2] = cov.tt;

                //gsl_eigen_symmv (m, eval,  evec, m_eigenspace);
                Stat.eigen_decomposition(m, evec, eval);
                
                //cout << "evals=" << gsl_vector_get(eval, 0) <<  " " << gsl_vector_get(eval, 1)<< " " << gsl_vector_get(eval, 2)<<endl;
                LOG.debug("evals=" + eval[0] +  " " + eval[1]+ " " + eval[2]);
                
                //gsl_matrix_free(m);
                //gsl_matrix_free(evec);
                //gsl_vector_free(eval);
            } else {
                if (useICP){
                    LOG.debug("USING ICP");
                    score=m_matcher.icpOptimize(newPose, m_map, m_pose, plainReading);
                }else
                    score=m_matcher.optimize(newPose, m_map, m_pose, plainReading);
            }


        }
        //...and register the scan
        if (m_count == 0 || score<m_regScore){
            
            LOG.debug("Registering");
            m_matcher.invalidateActiveArea();
            if (score<m_critScore){
                
                LOG.debug("New Scan added, using odo pose");
                m_matcher.registerScan(m_map, m_pose, plainReading);
            } else {
                m_matcher.registerScan(m_map, newPose, plainReading);
                
                LOG.debug("New Scan added, using matched pose");
            }
        }

        
        LOG.debug(" FinalPose: x="
                + newPose.x + " y=" + newPose.y + " theta=" + newPose.theta );
        LOG.debug("score=" + score);
        m_pose=newPose;
        m_count++;
    }
}
