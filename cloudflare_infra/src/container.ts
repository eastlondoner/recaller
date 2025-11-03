import { Container } from "@cloudflare/containers";
import type { worker } from "../alchemy.run.ts";


export class VideoTranscoderContainer extends Container {
  declare env: typeof worker.Env;

  override defaultPort = 8080; // The port server.py listens on
  override sleepAfter = process.env.NODE_ENV?.toLowerCase().startsWith("dev") ? "10M" : "10s"; // Sleep the container if no requests are made in this timeframe

  override onStart() {
    console.log("Video transcoder container successfully started");
  }

  override onStop() {
    console.log("Video transcoder container successfully shut down");
  }

  override onError(error: unknown) {
    console.log("Video transcoder container error:", error);
  }
}

