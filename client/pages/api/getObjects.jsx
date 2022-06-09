import { gql } from '@apollo/client';

const GET_OBJECTS = gql`
  query Objects($options: ObjectOptions) {
    objects(options: $options) {
      id
      file_uri
    }
  }
`;

export { GET_OBJECTS };
