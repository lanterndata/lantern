#ifndef LDB_HNSW_DISK_INTERFACE_H
#define LDB_HNSW_DISK_INTERFACE_H
// THIS FILE IS CURRENTLY UNSUSED.
// This just outlines the direction by which we will improve external_index.*

// implements an array data structure backed by postgres WAL
// saves memory [obj, obj + obj_size) to disk in postgres
// if obj_id > 0, asserts that an object with obj_id-1 is already saved and there is
// no object with obj_id
void append_object(void *obj, int obj_size, int obj_id);

void *get_object(int obj_id, int obj_size);

/*
the above works well for objects that own all of their data. E.g.
typedef struct {
    int a;
    int b;
    int c;
} point;
point center;

can be saved with append_object(&center, sizeof(center), 0);
but what if the object I am saving has pointers to other objects?
I want an interface to serialize the whole thing into the same disk block
as the whole thing is likely accessed together.
Example:

typedef struct {
    char* meta;
    char* vector;
} point;
point center;

This can be saved and restored with the two interfaces below:
object_member_map center_map[] = {
    {"meta", offsetof(point, meta), 156},
    {"vector", offsetof(point, vector), 512 * 8},
};
append_object(tree, sizeof(tree), 0, center_map, lengthof(center_map));
*/
typedef struct object_member_map
{
    // for debugging. will remove later
    // pointer stays owned by the caller
    char *name;
    // offset within the passed object which should be interpreted as memory pointer
    // during saving, the contents of this pointer[0, size) will be saved
    // during reconstruction, the field offset will be set to a postgres-owned memory containing the data
    int offset;
    // length of the object pointed to by pointer at offset
    int size;
} object_member_map;

void append_object_tree(void *obj, int obj_size, int obj_id, object_member_map *map, int map_size);
// map is used for validation. we have internally stored that info as well
void *get_object_tree(int obj_id, int obj_size, object_member_map *map, int map_size);

#endif // LDB_HNSW_DISK_INTERFACE_H