// includes, kernels
#include <stdio.h>
#include <assert.h>
#define NUM_ELEMENTS 512
// **===------------------------------------------------------------------===**
//! @param g_idata  input data in global memory
//                  result is expected in index 0 of g_idata
//! @param n        input number of elements to scan from input data
// **===------------------------------------------------------------------===**
__global__ void reduction(float *g_data, int n)
{
  int stride;
  // Define shared memory
  __shared__ float scratch[NUM_ELEMENTS];
  // Load the shared memory
  scratch[threadIdx.x ] = g_data[threadIdx.x];
  if(threadIdx.x + blockDim.x < n)
    scratch[threadIdx.x + blockDim.x] = g_data[threadIdx.x + blockDim.x];
  __syncthreads();
  // Do sum reduction from shared memory
  for(stride = 1 ; stride < blockDim.x; stride *= 2)
  {
    __syncthreads();
    if(threadIdx.x % (2*stride) == 0)
        scratch[threadIdx.x] += scratch[threadIdx.x + stride];
  }
  // Store results back to global memory
  if(threadIdx.x == 0)
    g_data[0] = scratch[0];
return; 
}

////////////////////////////////////////////////////////////////////////////////
// Program main
////////////////////////////////////////////////////////////////////////////////
void runTest( int argc, char** argv);
float computeOnDevice(float* h_data, int array_mem_size);
extern "C" void computeGold( float* reference, float* idata, const unsigned int len);
int main( int argc, char** argv)
{
    runTest( argc, argv);
    return EXIT_SUCCESS;
}
////////////////////////////////////////////////////////////////////////////////
//! Run naive scan test
////////////////////////////////////////////////////////////////////////////////
void runTest( int argc, char** argv)
{
    int num_elements = NUM_ELEMENTS;
    const unsigned int array_mem_size = sizeof( float) * num_elements;
    // allocate host memory to store the input data
    float* h_data = (float*) malloc( array_mem_size);
    // * No arguments: Randomly generate input data and compare against the host's
            // initialize the input data on the host to be integer values
            // between 0 and 1000
            for( unsigned int i = 0; i < num_elements; ++i)
            {
                //h_data[i] = floorf(1000*(rand()/(float)RAND_MAX));
                h_data[i] = i*1.0;
}
        // compute reference solution
    float reference = 0.0f;
    computeGold(&reference , h_data, num_elements);
    float result = computeOnDevice(h_data, num_elements);
    // We can use an epsilon of 0 since values are integral and in a range
    // that can be exactly represented
    float epsilon = 0.0f;
    unsigned int result_regtest = (abs(result - reference) <= epsilon);
    printf( "Test %s\n", (1 == result_regtest) ? "PASSED" : "FAILED");
    printf( "device: %f  host: %f\n", result, reference);
    // cleanup memory
    free( h_data);
}
/////////////////////////////////////////////////////////////////////////
// Take h_data from host, copies it to device, setup grid and thread
 // dimentions, excutes kernel function, and copy result of scan back
 // to h_data.
 // Note: float* h_data is both the input and the output of this function.
 /////////////////////////////////////////////////////////////////////////
 float computeOnDevice(float* h_data, int num_elements)
 {
   float* d_data = NULL;
   float result;
   // Memory allocation on device side
   cudaMalloc((void**)&d_data, num_elements*sizeof(float));
   // Copy from host memory to device memory
   cudaMemcpy(d_data, h_data, num_elements*sizeof(float), cudaMemcpyHostToDevice);
   //int threads = (num_elements/2) + num_elements%2;
   int threads = num_elements;
   // Invoke the kernel
   reduction<<<1,threads>>>(d_data,num_elements);
   // Copy from device memory back to host memory
   cudaMemcpy(&result, d_data, sizeof(float), cudaMemcpyDeviceToHost);
   cudaFree(d_data);
   return result;
}
 ///////////////////////////////////////////////////////////////////////
 void computeGold( float* reference, float* idata, const unsigned int len)
 {
   reference[0] = 0;
   double total_sum = 0;
   unsigned int i;
   for( i = 0; i < len; ++i)
   {
       total_sum += idata[i];
   }
   reference[0] = total_sum;
 }
 ///////////////////////////////////////////////////////////////////////