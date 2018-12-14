package edu.iu.dsc.constant;

public final class Constants {

    private Constants() {

    }

    /**
     * TODO : Please add the definition of the new flag added in the development process
     * */

    /**
     * itr : Iterations in the application
     * operation : Operation type reduce, gather, etc
     * parallelism  : number of parallel processes running
     * stages : size of the parallel operation stages 8,8 meaning divided into 8 components
     * fname : filename
     * exp : experiment name [distinguish multiple running examples]
     * input : input directory from where data is loaded
     * output : output directory to where the data is being saved
     * stream : use this flag when the application needs to be run in streaming mode, without this flag the applications run in batch mode
     * threads : number of threads used in the thread pools [used if the examples are executed using the executor layer
     * pi : log printing frequency
     * dtype : data type used in the application , i.e DOUBLE, INT, etc [allcaps]
     * verify : verify the results with expected results [important for validating final result known examples] {optional flag}
     * nFiles : number of files in the loading [important to understand the partition logic]
     * fShared : this flag specify to run the application with shared file system TODO: correct this if wrong     *
     * keyed : -keyed choose to run keyed operations based example, -op reduce -keyed i.e this will run keyed-reduce operation
     * app : -app terasort , this will run terasort application.
     * comms : -comms specify that the communication based examples are being run
     * taske : -task task executor based examples are being run.
     * */


    public static final String ARGS_ITR = "itr";
    public static final String ARGS_SIZE = "size";
    public static final String ARGS_OPERATION = "op";
    public static final String ARGS_PARALLELISM = "parallelism";
    public static final String ARGS_TASKS = "tasks";
    public static final String ARGS_TASK_STAGES = "stages";
    public static final String ARGS_FNAME = "fname";
    public static final String ARGS_EXP_NAME = "exp";
    public static final String ARGS_INPUT_DIRECTORY = "input";
    public static final String ARGS_OUTPUT_DIRECTORY = "output";
    public static final String ARGS_STREAM = "stream";
    public static final String ARGS_THREADS = "threads";
    public static final String ARGS_PRINT_INTERVAL = "pi";
    public static final String ARGS_DATA_TYPE = "dtype";
    public static final String ARGS_INIT_ITERATIONS = "int_itr";
    public static final String ARGS_VERIFY = "verify";
    public static final String ARGS_NUMBER_OF_FILES = "nFiles";
    public static final String ARGS_SHARED_FILE_SYSTEM = "fShared";
    public static final String ARGS_KEYED = "keyed";
    public static final String ARGS_APP = "app";
    public static final String ARGS_COMMS = "comms";
    public static final String ARGS_TASK_EXEC = "taske";
    public static final String ARGS_GAP = "gap";
    public static final String ARGS_OUTSTANDING = "outstanding";
    public static final String ARGS_APP_NAME = "appName";

}
