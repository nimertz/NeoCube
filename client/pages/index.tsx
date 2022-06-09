import { gql } from '@apollo/client';
import { NumberInput } from '@mantine/core';
import React, { useEffect, useState } from "react";
import { ColorSchemeToggle } from '../components/ColorSchemeToggle/ColorSchemeToggle';
import { ImageGrid } from '../components/ImageGrid/ImageGrid';
import client from './api/client';

export default function HomePage({ objects }) {
  const [value, setValue] = useState(100);
  return (
    <>
      <ColorSchemeToggle />
      <NumberInput
        defaultValue={18}
        placeholder="Limit"
        label="Media limit"
        value={value}
        onChange={(val) => setValue(val)}
      />
      />
      <ImageGrid objects={objects} />
    </>
  );
}

export async function getStaticProps() {
  const limit = 5;
  const offset = 0;

  const { data } = await client.query({
    query: gql`
      query Objects($options: ObjectOptions) {
        objects(options: $options) {
          id
          file_uri
        }
      }
    `,
    variables: { options: { limit, offset } },
  });

  //console.log(data);

  return {
    props: {
      objects: data?.objects,
    },
  };
}
