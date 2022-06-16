import { Grid } from '@mantine/core';
import { ImageItem } from '../ImageItem/ImageItem';

export function ImageGrid({ objects }) {
  return (
    <Grid grow>
      {objects.map((o) => (
        <Grid.Col key={o.id} span={3}>
          <ImageItem src={o.file_uri} />
        </Grid.Col>
      ))}
    </Grid>
  );
}
