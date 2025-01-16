# douga
unofficial implementation of the bluesky video service, for the bit

useful to AppViews that are based off the main bluesky appview.

keep in mind that at the moment (january 2025), bsky video is a centralized service living on https://video.bsky.app.
in turn, to make this work within the atproto mainnet, you and your friends need to use the same build of social-app
that uses your own video service. more information in the **how** section

[![Watch the video](https://smooch.computer/i/yjm4k7tjtfyo5.png)](https://smooch.computer/i/bmd9paf76z2.mp4)

## how

- get go1.23 because indigo depends on it: https://go.dev/dl/

```
git clone https://github.com/lun-4/douga
cd douga
env FRONTEND_URL=https://fe.example.net
	APPVIEW_URL=https://appview.example.net \
	ATPROTO_PLC_URL=https://plc.example.net \
	SERVER_URL=video.example.net \
	PORT=43093 go run .
```

then set video.example.net as the service inside social-app, this requires social-app patches.

`EXPO_PUBLIC_BSKY_VIDEO_HOSTNAME=video.example.net`

more info on: https://l4.pm/wiki/Personal%20Wiki/bluesky/bsky%20independent%20appview/a%20step%20by%20step%20process%20into%20an%20appview.html#social-app
