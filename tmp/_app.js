import Layout from '../components/layout'
import '../styles/globals.css'
import NextNProgress from 'nextjs-progressbar';
import Script from "next/script";
import Head from 'next/head'

function MyApp({ Component, pageProps }) {

  return (
    <>

<Script
        id="gtag-script-1"
        strategy="lazyOnload"
        src={`https://www.googletagmanager.com/gtag/js?id=${process.env.NEXT_PUBLIC_GOOGLE_ANALYTICS}`}
      />

      <Script id="gtag-script-2" strategy="lazyOnload">
        {`
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', '${process.env.NEXT_PUBLIC_GOOGLE_ANALYTICS}', {
              page_path: window.location.pathname,
            });
                `}
      </Script>
      <Head>
      <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-6967742946690048"
     crossOrigin="anonymous"></script>
      </Head>
      

    <Layout>
    <NextNProgress
    options={{ easing: 'ease', speed: 500,showSpinner: false }}
    color="#6468fc"
    startPosition={0.3}
    stopDelayMs={200}
    height={3}
    showOnShallow={false}
  />
    <Component {...pageProps} />
    
    </Layout>

    </>
  )
}

export default MyApp
