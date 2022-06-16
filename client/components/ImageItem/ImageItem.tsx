import { Image } from '@mantine/core';

export function ImageItem({ src }: string) {
  return (
    <div style={{ width: 400, marginLeft: 'auto', marginRight: 'auto' }}>
      <Image
        radius="md"
        src={"http://bjth.itu.dk:5008/" + src}
        alt={"http://bjth.itu.dk:5008/" + src}
      />
    </div>
  );
}
