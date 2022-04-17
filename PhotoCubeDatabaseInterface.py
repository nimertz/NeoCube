class PhotoCubeDB:
    def get_name(self):
        """Get the name of the database"""
        pass

    def close(self):
        """Close the database connection"""
        pass
    
    def get_tag_by_id(self,tag_id):
        """Get a tag by its id"""
        pass

    def get_tags_in_tagset(self,tagset_id):
        """Get all tags in a tagset"""
        pass

    def execute_query(self,query):
        """Execute a query from string"""
        pass

    def gen_state_query(numdims, numtots, types, filts):
        """Generate a query for the state of the cube"""
        pass

    def get_level_from_parent_node(self,node_id,hierarchy_id):
        """Get the level beneath a node in a hierarchy"""
        pass
