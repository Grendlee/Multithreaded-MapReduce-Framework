#define _DEFAULT_SOURCE

#include "interface.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utarray.h>
#include <uthash.h>

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

UT_icd cust_mr_in_kv = {sizeof(struct mr_in_kv), NULL, NULL, NULL};
UT_icd cust_mr_out_kv = {sizeof(struct mr_out_kv), NULL, NULL, NULL};

UT_array *mapped_array;
UT_array *reduced_array;

struct table_kv {
  char key[MAX_KEY_SIZE];
  UT_array *values;
  UT_hash_handle hh;
};

struct table_kv *hash = NULL;
struct table_kv *hash_final = NULL;
struct map_args {
  const struct mr_input *input;
  void (*map)(const struct mr_in_kv *);
  size_t mapper_count;
  void (*reduce)(const struct mr_out_kv *);
  size_t reducer_count;
  struct mr_output *output;
  size_t input_start_index;
  size_t input_end_index;
};

int compare_intermediate(const void *left, const void *right) {
  struct mr_in_kv *left_kv = (struct mr_in_kv *)left;
  struct mr_in_kv *right_kv = (struct mr_in_kv *)right;

  return strcmp(left_kv->key, right_kv->key);
}

void free_table() {
  struct table_kv *kv_p;

  struct table_kv *alt;
  if (hash != NULL) {
    HASH_ITER(hh, hash, kv_p, alt) {

      utarray_free(kv_p->values);
      HASH_DEL(hash, kv_p);
      free(kv_p);
    }
    hash = NULL;
  }
}

void print_hash_table() {
  struct table_kv *currentEntry;
  struct table_kv *tmpIter;

  printf("Hash Table:\n");
  fflush(stdout);

  printf("------------------------------------\n");
  fflush(stdout);

  HASH_ITER(hh, hash, currentEntry, tmpIter) {
    printf("key: %s, ", currentEntry->key);
    fflush(stdout);
    printf("Values: ");
    fflush(stdout);

    for (size_t i = 0; i < utarray_len(currentEntry->values); i++)

    {
      char *value = (char *)utarray_eltptr(currentEntry->values, i);
      if (value != NULL) {
        printf("%s , ", value);
        fflush(stdout);
      }
    }
    printf("\n");
  }
}
void group_by_key() {
  struct mr_in_kv *kv_p = NULL;

  UT_icd str_icd = {sizeof(char[MAX_VALUE_SIZE]), NULL, NULL, NULL};

  if (mapped_array == NULL) {
    perror("mapped array is null in group by key");
    return;
  }
  for (size_t i = 0; i < utarray_len(mapped_array); i++)

  {

    kv_p = (struct mr_in_kv *)utarray_eltptr(mapped_array, i);
    if (kv_p == NULL) {
      perror("kv_p in group by key NULL");
      continue;
    }
    struct table_kv *exists;
    HASH_FIND_STR(hash, kv_p->key, exists);

    // found add value to values
    if (exists != NULL) {
      utarray_push_back(exists->values, kv_p->value);
    }

    // not found add to hash table
    if (exists == NULL) {
      // add key value pair to the hash table
      exists = (struct table_kv *)calloc(1, sizeof(struct table_kv));
      if (exists == NULL) {
        continue;
      }
      // copy key to hash struct
      strncpy(exists->key, kv_p->key, MAX_KEY_SIZE);

      // put nut term
      // exists->key[MAX_KEY_SIZE - 1] = '\0';

      // new array for values
      utarray_new(exists->values, &str_icd);

      if (exists->values == NULL) {
        free(exists);
        continue;
      }
      // add value to array of values
      utarray_push_back(exists->values, kv_p->value);
      // add kv to hastable
      HASH_ADD_STR(hash, key, exists);
    }
  }
}

void *reducer_func(void *args) {

  struct map_args *arg = args;
  size_t i = 0;
  struct table_kv *cur, *tmp;

  // printf("in reducer\n");
  // fflush(stdout);
  HASH_ITER(hh, hash, cur, tmp) {

    if (i >= arg->input_start_index && i < arg->input_end_index) {
      // look at this hash table entry
      // add the key to the hash_end table
      // iterate through the values of this key and sum them up
      // then assign the value of the hash_end table key to this sum
      // char key[max key size
      // char (*value)[max_value_size]
      // size_t count
      //
      struct mr_out_kv entry_reduce;

      pthread_mutex_lock(&mutex);
      strncpy(entry_reduce.key, cur->key, MAX_KEY_SIZE);
      // entry_reduce.key[MAX_KEY_SIZE - 1] = '\0';

      entry_reduce.count = utarray_len(cur->values);
      entry_reduce.value =
          malloc(entry_reduce.count * sizeof(char[MAX_VALUE_SIZE]));

      if (entry_reduce.value == NULL) {

        return NULL;
      }
      memcpy(entry_reduce.value, utarray_front(cur->values),
             entry_reduce.count * sizeof(char[MAX_KEY_SIZE]));
      pthread_mutex_unlock(&mutex);

      arg->reduce(&entry_reduce);

      free(entry_reduce.value);
    }

    i++;
  }

  pthread_exit(0);
}

// takes in args
// calls map with correct mr_in_kv in_kv
void *mapper_func(void *args) {

  // printf("in mapper");
  // fflush(stdout);
  struct map_args *arg = args;
  for (size_t i = arg->input_start_index; i < arg->input_end_index; i++) {

    arg->map(&arg->input->kv_lst[i]);
  }

  pthread_exit(0);
}

void print_mapped_array() {
  for (size_t i = 0; i < utarray_len(mapped_array); i++) {
    struct mr_in_kv *pair = (struct mr_in_kv *)utarray_eltptr(mapped_array, i);
    if (pair == NULL) {
      break;
    }
    // printf("Key: %s, Val: %s\n", pair->key, pair->value);
    // fflush(stdout);
  }
}
void print_reduced_array() {
  for (size_t i = 0; i < utarray_len(reduced_array); i++) {
    struct mr_in_kv *pair = (struct mr_in_kv *)utarray_eltptr(reduced_array, i);
    if (pair == NULL) {
      break;
    }
    // printf("Key: %s, Val: %s\n", pair->key, pair->value);

    // fflush(stdout);
  }
}

void clean() {
  if (reduced_array != NULL) {
    utarray_free(reduced_array);
  }

  if (mapped_array != NULL) {
    utarray_free(mapped_array);
  }

  free_table();

  hash = NULL;
  hash_final = NULL;
  mapped_array = NULL;
  reduced_array = NULL;
}

int mr_exec(const struct mr_input *input, void (*map)(const struct mr_in_kv *),
            size_t mapper_count, void (*reduce)(const struct mr_out_kv *),
            size_t reducer_count, struct mr_output *output) {
  if (input == NULL || input->count == 0 || input->kv_lst == NULL ||
      map == NULL || reduce == NULL || output == NULL) {
    return -1;
  }

  // printf("Mapper Count: %zu\n", mapper_count);
  // printf("Reducer Count: %zu\n", reducer_count);
  // fflush(stdout);
  output->kv_lst = NULL;
  output->count = 0;

  utarray_new(mapped_array, &cust_mr_in_kv);
  utarray_new(reduced_array, &cust_mr_in_kv);

  pthread_t array_of_mappers[mapper_count];
  pthread_t array_of_reducers[reducer_count];

  size_t size_of_chunks_map = input->count / mapper_count;
  size_t size_of_chunks_red = input->count / reducer_count;
  // size_t elems_in_last_chunk_map = input->count % mapper_count;
  // size_t elems_in_last_chunk_red = input->count % mapper_count;

  struct map_args arg[mapper_count];
  for (size_t i = 0; i < mapper_count; i++) {
    int end_index;
    if (i != mapper_count - 1) {
      end_index = size_of_chunks_map * (i + 1);
    } else {
      end_index = input->count;
    }

    arg[i].input = input;
    arg[i].map = map;
    arg[i].mapper_count = mapper_count;
    arg[i].reduce = reduce;
    arg[i].reducer_count = reducer_count;
    arg[i].output = output;
    arg[i].input_start_index = i * size_of_chunks_map;
    arg[i].input_end_index = end_index;

    // printf("partition: %zu\n", mapper_count);
    // printf("start index: %zu\n", i * size_of_chunks_map);
    // printf("end index: %d\n", end_index);
    if (pthread_create(&array_of_mappers[i], NULL, mapper_func, &arg[i]) != 0) {
      perror("pthread_create-mapper1");
      // printf("mapcreate\n");
      // fflush(stdout);
      utarray_free(mapped_array);
      utarray_free(reduced_array);
      return -1;
    }
  }
  // printf("end of mapper_count loop\n");
  // fflush(stdout);

  for (size_t i = 0; i < mapper_count; i++) {
    if (pthread_join(array_of_mappers[i], NULL) != 0) {
      perror("pthread join ");
      // printf("pthreadjoin\n");

      fflush(stdout);

      utarray_free(mapped_array);
      utarray_free(reduced_array);
      return -1;
    }
  }
  // printf("end of mapper join loop\n");
  // fflush(stdout);

  if (mapped_array == NULL) {

    // printf("Mapped array NULL\n\n\n\n\n\n\n\n\n\n");
    // fflush(stdout);
  } else {

    print_mapped_array();
    // printf("Mapped array NOT NULL\n\n\n\n\n\n\n\n\n\n");
    // fflush(stdout);
    // printf("end_index: %zu\n", arg[0].input_end_index);
    // fflush(stdout);

    struct mr_in_kv *pair = (struct mr_in_kv *)utarray_eltptr(mapped_array, 0);

    if (pair == NULL) {

      // printf("null pairs?");
      // fflush(stdout);
      //  return -1;

    } else {

      utarray_sort(mapped_array, compare_intermediate);
    }
  }
  // print_mapped_array();
  group_by_key();
  // print_hash_table();
  //

  // run threads for reducer
  //

  // printf("start of reducer\n");
  // fflush(stdout);

  struct map_args arg_red[reducer_count];
  for (size_t i = 0; i < reducer_count; i++) {
    int end_index;
    if (i != reducer_count - 1) {
      end_index = size_of_chunks_red * (i + 1);
    } else {
      end_index = input->count;
    }

    arg_red[i].input = input;
    arg_red[i].map = map;
    arg_red[i].mapper_count = reducer_count;
    arg_red[i].reduce = reduce;
    arg_red[i].reducer_count = reducer_count;
    // arg_red[i].output = output;
    arg_red[i].input_start_index = i * size_of_chunks_red;
    arg_red[i].input_end_index = end_index;

    if (pthread_create(&array_of_reducers[i], NULL, reducer_func,
                       &arg_red[i]) != 0) {
      perror("pthread_create-reducer");
      utarray_free(mapped_array);
      utarray_free(reduced_array);
      free_table();
      return -1;
    }

    // printf("end of reduceCountloop\n");
    // fflush(stdout);
  }

  // printf("start of reducer join\n");
  // fflush(stdout);
  for (size_t i = 0; i < reducer_count; i++) {

    // printf("joinLoop start\n");
    // fflush(stdout);
    if (pthread_join(array_of_reducers[i], NULL) != 0) {

      // printf("before perror join\n");
      // fflush(stdout);
      perror("pthread join ");

      // printf(" join loop\n");
      // fflush(stdout);
      utarray_free(mapped_array);
      // free_table();
      return -1;
    }
  }

  // printf("start of print final kv pairs\n");
  // fflush(stdout);

  // print_reduced_array();

  utarray_sort(reduced_array, compare_intermediate);

  output->count = utarray_len(reduced_array);

  output->kv_lst = malloc(output->count * sizeof(struct mr_out_kv));
  if (output->kv_lst == NULL) {
    clean();
    return -1;
  }

  // now that we can an array called reduced array. We need to turn each element
  // from reduced array into @output, a struct containing an array of mr_out_kv
  // structs
  //
  //
  for (size_t i = 0; i < utarray_len(reduced_array); i++) {
    struct mr_in_kv *red = (struct mr_in_kv *)utarray_eltptr(reduced_array, i);
    if (red == NULL) {
      // prob no possible
      continue;
    }
    strncpy(output->kv_lst[i].key, red->key, MAX_KEY_SIZE);
    output->kv_lst[i].value = malloc(MAX_VALUE_SIZE);

    if (output->kv_lst[i].value == NULL) {
      return -1;
    }

    strncpy(output->kv_lst[i].value[0], red->value, MAX_VALUE_SIZE);
    output->kv_lst[i].count = 1;
  }

  clean();

  return 0;
}

int mr_emit_i(const char *key, const char *value) {
  if (key == NULL || value == NULL || mapped_array == NULL) {
    printf("mr_emit params NULL");
    return -1;
  }
  // printf("key: %s", key);
  pthread_mutex_lock(&mutex);

  struct mr_in_kv intermediate_value_pair;
  memset(&intermediate_value_pair, 0, sizeof(struct mr_in_kv));
  strncpy(intermediate_value_pair.key, key, MAX_KEY_SIZE);
  // intermediate_value_pair.key[MAX_KEY_SIZE - 1] = '\0';

  strncpy(intermediate_value_pair.value, value, MAX_VALUE_SIZE);

  // intermediate_value_pair.value[MAX_VALUE_SIZE - 1] = '\0';
  utarray_push_back(mapped_array, &intermediate_value_pair);
  pthread_mutex_unlock(&mutex);

  return 0;
}

int mr_emit_f(const char *key, const char *value) {

  if (key == NULL || value == NULL) {
    return -1;
  }
  pthread_mutex_lock(&mutex);
  struct mr_in_kv intermediate_value_pair;
  memset(&intermediate_value_pair, 0, sizeof(struct mr_in_kv));
  strncpy(intermediate_value_pair.key, key, MAX_KEY_SIZE);
  // intermediate_value_pair.key[MAX_KEY_SIZE - 1] = '\0';

  strncpy(intermediate_value_pair.value, value, MAX_VALUE_SIZE);

  // intermediate_value_pair.value[MAX_VALUE_SIZE - 1] = '\0';
  utarray_push_back(reduced_array, &intermediate_value_pair);
  pthread_mutex_unlock(&mutex);

  return 0;
}
