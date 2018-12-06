package pe.com.belcorp.datalake.analytics

import pe.com.belcorp.datalake.analytics.addons.{CreateFromResource, UpdateFromResource}

trait OldTable extends Table with CreateFromResource with UpdateFromResource
