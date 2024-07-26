package config

import (
	"dts/prisma/db"

	"github.com/rs/zerolog/log"
)

func ConnectDb() (*db.PrismaClient, error) {
	client := db.NewClient()
	err := client.Prisma.Connect()
	if err != nil {
		return nil, err
	}

	log.Info().Msg("Connected to database")

	return client, nil
}
