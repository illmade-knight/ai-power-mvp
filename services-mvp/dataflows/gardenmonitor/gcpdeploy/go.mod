module gcpdeploy

go 1.23.0

require (
	github.com/illmade-knight/ai-power-mpv/pkg v0.0.0
	github.com/rs/zerolog v1.34.0
)

replace (
	github.com/illmade-knight/ai-power-mpv/pkg => ../../../../pkg
)
