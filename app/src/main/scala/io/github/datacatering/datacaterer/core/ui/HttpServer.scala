//package io.github.datacatering.datacaterer.core.ui
//
//import io.github.datacatering.datacaterer.core.ui.material.mdFilledButton
//import zio.{ZIO, ZIOAppDefault}
//import zio.http.{Body, Handler, Header, Headers, HttpApp, MediaType, Method, Request, Response, Routes, Server, handler}
//import zio.http.template.{a, altAttr, b, body, classAttr, contentAttr, div, form, h2, head, hrefAttr, html, httpEquivAttr, idAttr, img, link, meta, nav, relAttr, s, script, span, srcAttr, title, typeAttr}
//import zio.stream.ZStream
//
//object HttpServer extends ZIOAppDefault {
//
//  private val dataCateringIcon = "report/data_catering_transparent.svg"
//  private val mainCss = "ui/main.css"
//  private val indexJs = "ui/index.js"
//  private val indexImportJs = "ui/index-import.js"
//
//  val app: HttpApp[Any] =
//    Routes(
//      Method.GET / "" -> handler { (req: Request) =>
//        req.body.asMultipartForm.map(f => f.formData)
//        Response.html(html(
//          head(
//            meta(httpEquivAttr := "Content-Type", contentAttr := "text/html; charset=utf-8"),
//            title(idAttr := "title", "Data Caterer"),
//            script(srcAttr := "https://esm.run/@material/web/all.js", typeAttr := "module"),
//            link(relAttr := "stylesheet", hrefAttr := "https://fonts.googleapis.com/css2?family=Open%20Sans:wght@400;500;700&display=swap", typeAttr := "text/css"),
//            link(relAttr := "stylesheet", hrefAttr := "https://fonts.googleapis.com/icon?family=Material+Symbols+Rounded"),
//            link(relAttr := "stylesheet", hrefAttr := mainCss, typeAttr := "text/css"),
//          ),
//          body(
//            div(
//              classAttr := "top-banner" :: Nil,
//              a(
//                classAttr := "logo" :: Nil, hrefAttr := "/",
//                img(srcAttr := dataCateringIcon, altAttr := "logo")
//              ),
//              span(b("Data Caterer"))
//            ),
//            nav(
//              classAttr := "topnav" :: Nil,
//              a("Home"),
//              a("Run"),
//              a("History"),
//            ),
//            div(
//              h2("Create new plan"),
//              form(
//                idAttr := "plan-form",
//                classAttr := "plan-form" :: Nil,
//                div(
//                  idAttr := "data-sources",
//                  div(
//                    mdFilledButton("+ Data source", idAttr := "add-data-source-button", typeAttr := "button")
//                  )
//                ),
//                mdFilledButton("Submit", idAttr := "submit-plan")
//              )
//            ),
//            script(srcAttr := indexJs),
//          )
//        ))
//      },
//      Method.GET / dataCateringIcon -> createHandler(dataCateringIcon),
//      Method.GET / mainCss -> createHandler(mainCss),
//      Method.GET / indexJs -> createHandler(indexJs),
//      Method.GET / indexImportJs -> createHandler(indexImportJs),
//      Method.GET / "health" -> handler(Response.ok),
//      Method.POST / "run" -> handler { (req: Request) =>
//        req.body.asString.map(body => {
//          println(body)
//          Response.text(body)
//        }).mapError(err => {
//          Response.text(s"Failed to run job, error message: ${err.getMessage}")
//        })
//      }
//    ).toHttpApp
//
//  override val run: ZIO[Any, Throwable, Nothing] = Server.serve(app).provide(Server.default)
//
//  private def createHandler(fileName: String): Handler[Any, Nothing, Any, Response] = {
//    val extension = fileName.split("\\.").last
//    handler(Response(
//      headers = Headers(Header.ContentType(MediaType.forFileExtension(extension).get)),
//      body = Body.fromStream(ZStream.fromResource(fileName))
//    ))
//  }
//
//}
//
