class M3DB:
    def get_name(self):
        """Get the name of the database"""
        pass

    def close(self):
        """Close the database connection"""
        pass

    def get_tag_by_id(self, tag_id):
        """Get a tag by its id"""
        pass

    def get_tags_in_tagset(self, tagset_id):
        """Get all tags in a tagset"""
        pass

    def gen_cell_query(self, numdims, numtots, types, filts):
        """Get a cell (objects associated with tag/node filters)"""
        pass

    def execute_query(self, query):
        """Execute a query from string - readonly"""
        pass

    @staticmethod
    def gen_state_query(numdims, numtots, types, filts):
        """Generate a querystring for the state of the cube"""
        pass

    def get_level_from_parent_node(self, node_id, hierarchy_id):
        """Get the level beneath a node in a hierarchy"""
        pass

    def get_node_tag_subtree(self, node_id):
        """Get all tags in a subtree of a node"""
        pass

    def get_objects_with_tag(self, tag_id):
        """Get all objects with a tag"""
        pass

    def get_objects_in_tagset(self, tagset_id):
        """Get all objects and the tags they are associated with in a given tagset"""
        pass

    def get_objects_in_subtree(self, node_id):
        """Get all objects and the tags they are associated with in a given subtree"""
        pass

    def insert_object(self, id, file_uri, file_type, thumbnail_uri):
        """Insert an object into the database"""
        pass

    def insert_tag(self, id, name, tagtype_id, tagset_id):
        """Insert a tag into the database"""
        pass

    def insert_tagset(self, id, name):
        """Insert a tagset into the database"""
        pass

    def insert_node(self, id, tag_id, hierarchy_id):
        """Insert a node into the database"""
        pass

    def tag_object(self, object_id, tag_id):
        """Tag an object with a tag"""
        pass

    def update_object(self, id, file_uri, file_type, thumbnail_uri):
        """Update an object in the database"""
        pass

    def update_tag(self, id, name, tagtype_id, tagset_id):
        """Update a tag in the database"""
        pass

    def set_autocommit(self, autocommit):
        """Set autocommit on or off"""
        pass

    def rollback(self):
        """Rollback the current transaction"""
        pass

    def refresh_all_views(self):
        """Refresh all views in the database"""
        pass

    def refresh_object_views(self):
        """Refresh the views depending on object in the database"""
        pass

    def delete_all_benchmark_data(self):
        """Delete all benchmark data from the database"""
        pass


