export interface Track {
  artist: string;
  song: string;
  reason: string;
  spotify_url?: string | null;
  album_image_url?: string | null;
  release_year?: number | null;
}

export interface InfluenceEdge {
  from: string;
  to: string;
}

export interface CreditEntity {
  name: string;
  role: string;
}

export interface EquipmentEntity {
  name: string;
  category: string;
}

export interface Playlist {
  title: string;
  description: string;
  tracks: Track[];
  tags?: string[];
  countries?: string[];
  cities?: string[];
  studios?: string[];
  venues?: string[];
  scenes?: string[];
  influences?: InfluenceEdge[];
  credits?: CreditEntity[];
  equipment?: EquipmentEntity[];
}
