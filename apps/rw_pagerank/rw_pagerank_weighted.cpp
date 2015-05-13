#include <string>
#include <graphlab.hpp>
#include <math.h>


double prob_to_start_rw;
int num_of_rw_per_node_to_start;
int max_wait_iter;


struct vertex_data_type {

    unsigned int pagerank;
    unsigned int rcv_msg; // number of frogs that vertex (w/o out-edges) accumulates

    void save(graphlab::oarchive& oarc) const {
        oarc << pagerank;
    }
    void load(graphlab::iarchive& iarc) {
        iarc >> pagerank;
    }
};

typedef float edge_data_type; // edge is weighted

typedef int my_message_type;

// typedef graphlab::distributed_graph<vertex_data_type, graphlab::empty> graph_type;
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;

void init_vertex(graph_type::vertex_type& vertex) { vertex.data().pagerank = 0; }


class rw_pagerank_program : public graphlab::ivertex_program<graph_type, graphlab::empty,my_message_type>, public graphlab::IS_POD_TYPE {

    public:
        int received_msg;
        float frac_of_active_replicas;//this variable will be set by the engine
    
        void init(icontext_type& context, vertex_type& vertex, const my_message_type& msg){
            if(msg == -1){ // in the very first iteration: spread out frogs across all vertices evenly
                received_msg = num_of_rw_per_node_to_start + ((graphlab::random::rand01() < prob_to_start_rw)? 1 : 0);
		context.cout() << "[init] vertex: " << vertex.id() << std::endl;
		context.cout() << "\t rcv_msg (-1): " << received_msg << std::endl;
	    }
            else{ // for later iterations: received_msg contains SUMMED number of frogs
		  // received from ALL vertices that sent frogs
		  // (graphlab does this for us autamatically)
		received_msg = msg;
		context.cout() << "[init] vertex: " << vertex.id() << std::endl;
		context.cout() << "\t rcv_msg: " << received_msg << std::endl;
	    }

        }


        edge_dir_type gather_edges(icontext_type& context,
                const vertex_type& vertex) const {
            return graphlab::NO_EDGES;
        }


        void apply(icontext_type& context, vertex_type& vertex,
                const graphlab::empty& empty) {
	    context.cout() << "[apply] vertex: " << vertex.id() << std::endl;
	    context.cout() << "\t iter/max: " << context.iteration()+1 << "/" << max_wait_iter+1 << std::endl;
	    context.cout() << "\t # in-edges: " << vertex.num_in_edges() << std::endl;
	    context.cout() << "\t # out-edges: " << vertex.num_out_edges() << std::endl;
	   
	    if(vertex.num_out_edges() == 0){ // for nodes that have no out-edges:
					     // keep updating pagerank (number of frogs) 
		vertex.data().rcv_msg += received_msg;
		vertex.data().pagerank = vertex.data().rcv_msg;
		context.cout() << "\t pagerank (temp): " << vertex.data().pagerank << std::endl;
	    } // CONDITIONAL (1)
	    else
		context.cout() << "\t pagerank (temp): " << vertex.data().pagerank << std::endl;


	    if(context.iteration() >= max_wait_iter && received_msg > 0){ // when abort after max iteration:
									  // update pagerank
		vertex.data().pagerank = (vertex.num_out_edges() == 0)? vertex.data().rcv_msg : received_msg;
		context.cout() << "\t pagerank (final): " << vertex.data().pagerank << std::endl;
		received_msg = 0;
            } // CONDITIONAL (2)
	    
        }

	/*
	   for apply function:

		there are two cases where whole program is terminated
		1. after max iterations
		2. when there is NO vertex that receives frogs

		1. for vertices that have no out-edges,
			number of frogs that they have accumulated at the vertex is their pagerank
			see CONDITIONAL (1): they have collected them all along
		   for vertices that have out-edges, 
			number of frogs that they have at the time of termination is their pagerank
			see CONDITIONAL (2): they update their pagerank as current number of frogs

		2. for vertices that have no out-edges,
			they declare their pagerank to be number of frogs they have accumulated
			see CONDITIONAL (1): they have collected them all along
 		   for vertices that have out-edges,
			they declare their pagerank to be zero
			see CONDITIONAL (2): this conditional is not executed, their pagerank is zero
					     (was set in very beginning of whole program)
	*/

        edge_dir_type scatter_edges(icontext_type& context,
                const vertex_type& vertex) const { // 'scatter_edges' collects ALL edges
						   // on which 'scatter' method is executed IN PARALLEL
            if (vertex.num_out_edges() > 0 && received_msg > 0){ // only when have out-edges and frogs
                return graphlab::OUT_EDGES;
            }
            else {
		return graphlab::NO_EDGES;
	    }
        }


        void scatter(icontext_type& context, const vertex_type& vertex,
                edge_type& edge) const { // is executed in parallel on all edges
					 // collected by 'scatter_edges' that precedes 'scatter'

            int rw_to_send;
            float num_of_active_edges = (float)vertex.num_out_edges();

		context.cout() << "[scatter] edge: " << edge.source().id() << "->" << edge.target().id() << std::endl;
		context.cout() << "\t weight: " << edge.data() << std::endl;

            if(num_of_active_edges < 1){
                //float tmp = (float)received_msg / num_of_active_edges;
		float tmp = (float)received_msg * edge.data();
                int to_send_int = (int)tmp;
                rw_to_send = to_send_int + ((graphlab::random::rand01() < (tmp-to_send_int))? 1 : 0);
                context.signal(edge.target(),rw_to_send);
            }
            else{
                // float zero_prob = pow(1-1/num_of_active_edges,received_msg);
		float zero_prob = 0; // simplicity for now
                if(graphlab::random::rand01() >= zero_prob){
                    //float tmp = (float)received_msg / num_of_active_edges / (1-zero_prob);
		    float tmp = (float)received_msg * edge.data();
                    int to_send_int = (int)tmp;
                    rw_to_send = to_send_int + ((graphlab::random::rand01() < (tmp-to_send_int))? 1 : 0);
                    context.signal(edge.target(),rw_to_send);
                }
            }

        }
}; 



class graph_writer {
    public:
        std::string save_vertex(graph_type::vertex_type v) {
            std::stringstream strm;
            strm << v.id() << "\t" <<v.data().pagerank << "\n";
            return strm.str();
        }
        std::string save_edge(graph_type::edge_type e) { return ""; }
};

bool line_parser(graph_type& graph, 
                 const std::string& filename, 
                 const std::string& textline) { // per line: source, target, weight
  std::stringstream strm(textline);
  graphlab::vertex_id_type source, target;
  edge_data_type weight;

  strm >> source;
  strm >> target;
  strm >> weight;

  graph.add_edge(source, target, weight);
  return true;

}

/*
	'line_parser' is prone to bugs: it assumes graph_file is error-free
		graph_file should be:
			1. [source] [target] [weight]
			2. Per source, all out-edges' weights must add up to 1.
*/


int main(int argc, char* argv[]) {

    graphlab::command_line_options clopts("RW PageRank algorithm.");
    graphlab::graphlab_options rw_opts; 
    std::string graph_file;

    clopts.attach_option("graph", graph_file, "The graph file.");
    
    int rw_num = 800000;
    clopts.attach_option("rwnum", rw_num,
            "number of random walks (not including additional walks created for resets)");

    max_wait_iter = 5;
    clopts.attach_option("maxwait", max_wait_iter,
            "max num of iterations to wait");


    float replica_activation_prob = 1;
    clopts.attach_option("replicap", replica_activation_prob,
            "fraction of active replicas for scatter");


    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;
    graph_type graph(dc);
    global_logger().set_log_level(LOG_INFO);

    if(!clopts.parse(argc, argv)) {
        dc.cout() << "Error in parsing command line arguments." << std::endl;
        return EXIT_FAILURE;
    }

    if (graph_file.length() == 0) {
        dc.cout() << "No graph file given... exiting... " << std::endl;
        clopts.print_description();
        return 0;
    }


    rw_opts.engine_args.set_option("activation_prob",replica_activation_prob);
    rw_opts.engine_args.set_option("vertex_data_sync",true);
    
    max_wait_iter --;

    // graph.load_format(graph_file, "tsv");
    graph.load(graph_file, line_parser); // to include edge weights
    graph.finalize();

    num_of_rw_per_node_to_start = rw_num/graph.num_vertices();
    prob_to_start_rw = ((double)rw_num - graph.num_vertices()*num_of_rw_per_node_to_start)/graph.num_vertices();
    graph.transform_vertices(init_vertex);

    graphlab::synchronous_engine<rw_pagerank_program> engine_rw(dc, graph, rw_opts);

    if(max_wait_iter > -1) 
        engine_rw.signal_all(-1);

    engine_rw.start();

    const double runtime = engine_rw.elapsed_seconds();
    dc.cout() << "Finished RW Pagerank in " << runtime
        << " seconds." << " total MB sent by master:  "<< dc.network_megabytes_sent() << std::endl;


    dc.cout() << "num_of_rw_per_node_to_start: " << num_of_rw_per_node_to_start << std::endl;
    dc.cout() << "prob_to_start_rw: " << prob_to_start_rw << std::endl;

    graph.save("output",
            graph_writer(),
            false, // set to true if each output file is to be gzipped
            true, // whether vertices are saved
            false); // whether edges are saved


    graphlab::mpi_tools::finalize();


    std::ofstream myfile;
    char fname[20];
    sprintf(fname,"mystats_%d.txt",dc.procid());
    myfile.open (fname);
    double total_compute_time = 0;
    for (size_t i = 0;i < engine_rw.per_thread_compute_time.size(); ++i) {
        total_compute_time += engine_rw.per_thread_compute_time[i];
    }
    myfile << total_compute_time << "\t"<< dc.network_bytes_sent() <<"\t"<<runtime <<"\n";

    myfile.close();

    return EXIT_SUCCESS;
}





