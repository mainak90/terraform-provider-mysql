package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/plugin"
	mysql_provider "github.com/mainak90/terraform-provider-mysql/mysql-provider"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: mysql_provider.Provider,
	})
}
