// includes, kernels
#include <stdio.h>
#include <assert.h>
#define NUM_ELEMENTS 512
// **===------------------------------------------------------------------===**
//! @param g_idata  input data in global memory
//                  result is expected in index 0 of g_idata
//! @param n        input number of elements to scan from input data
// **===------------------------------------------------------------------===**
__global__ void reduction(float *g_data, const int n)
{
  // params
  int stride_size = ( n - blockDim.x +1) / blockDim.x; // ceil 
  int init = threadIdx.x * stride_size; 
  
  // Define shared memory
  __shared__ float scratch[NUM_ELEMENTS];
  // Load the shared memory
  // Alamcenamos nuestra posición y los contiguos 
  for(int i = init; i < init + stride_size && i < n; i++){
    scratch[i] = g_data[i]; // 
  }
  // Almacenamos los saltos 
  for (int stride = init+stride_size; stride < n; stride+= stride_size)
  {
    scratch[stride] = g_data[stride];
  }

  __syncthreads();
  // Do sum reduction from shared memory
  
  ///////  Reduction scheme 3 //////
  for(int i = init+1; i < init + stride_size && i < n; i++){
    scratch[init] += scratch[i]; // sumamos elementos contiguos 
  }
   __syncthreads(); // agrupamos
  
  // sumamos el resultado del bloque de la derecha
    if (init  + stride_size < n)
        scratch[ init ] +=  scratch[init  + stride_size];
  __syncthreads();
  
  // Store results back to global memory
  if(threadIdx.x == 0)
    g_data[0] = scratch[0];
return; 
}